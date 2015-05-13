package com.scopely.infrastructure.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class KinesisPlayer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisPlayer.class);

    private final VcrConfiguration vcrConfiguration;
    private final AmazonS3 s3;
    private final AmazonKinesis kinesis;

    public KinesisPlayer(VcrConfiguration vcrConfiguration,
                         AmazonS3 s3,
                         AmazonKinesis kinesis) {
        this.vcrConfiguration = vcrConfiguration;
        this.s3 = s3;
        this.kinesis = kinesis;

        // Check everything: S3 and Kinesis

        if (!s3.doesBucketExist(vcrConfiguration.bucket)) {
            LOGGER.error("Specified S3 bucket '{}' does not exist", vcrConfiguration.bucket);
            throw new IllegalArgumentException("Bucket not found");
        }

        try {
            kinesis.describeStream(vcrConfiguration.stream);
        } catch (ResourceNotFoundException e) {
            LOGGER.error("Specified Kinesis stream '{}' not found", vcrConfiguration.stream);
            throw e;
        }
    }

    public void play(LocalDate start, @Nullable LocalDate end) {
        playableObjects(start, end).stream().flatMap(summary -> {
            List<byte[]> kinesisPayloads = new LinkedList<>();
            try (S3Object s3Object = s3.getObject(summary.getBucketName(), summary.getKey())) {
                byte[] contents = IOUtils.toByteArray(s3Object.getObjectContent());
                int blockStart = 0;

                for (int position = 0; position < contents.length; position++) {
                    if (contents[position] == '\n') {
                        if (position == blockStart) {
                            continue;
                        }
                        // Copy out the range exclusive of our one-byte delimiter
                        kinesisPayloads.add(Arrays.copyOfRange(contents, blockStart, position));
                        blockStart = position + 1;
                    }
                }
                if (blockStart < contents.length) {
                    kinesisPayloads.add(Arrays.copyOfRange(contents, blockStart, contents.length));
                }

            } catch (IOException e) {
                LOGGER.error("Error reading object at key: " + summary.getKey(), e);
            }

            LOGGER.debug("Read {} records from object at key {}", kinesisPayloads.size(), summary.getKey());

            return kinesisPayloads.stream();
        })
                .map(b64Payload -> ByteBuffer.wrap(Base64.getDecoder().decode(b64Payload)))
                .map(payload -> kinesis.putRecord(vcrConfiguration.stream, payload, UUID.randomUUID().toString()))
                .forEach(result -> LOGGER.info("Wrote record. Seq {}, shard {}", result.getSequenceNumber(), result.getShardId()));
    }

    List<S3ObjectSummary> playableObjects(LocalDate start, @Nullable LocalDate end) {
        List<S3ObjectSummary> keys = new ArrayList<>();

        ObjectListing listing = s3.listObjects(vcrConfiguration.bucket, vcrConfiguration.stream + "/");
        do {
            listing.getObjectSummaries()
                    .stream()
                    .filter(summary -> {
                        if (!summary.getKey().startsWith(vcrConfiguration.stream)
                                || summary.getKey().length() < vcrConfiguration.stream.length() + 1 + "yyyy-MM-dd".length()) {
                            return false;
                        }

                        String date = summary.getKey().substring(vcrConfiguration.stream.length() + 1,
                                vcrConfiguration.stream.length() + 1 + "yyyy-MM-dd".length());

                        LocalDate fileDate = LocalDate.parse(date, S3RecorderPipeline.FORMATTER);

                        // We want objects with dates on or after the start, and, if there is a specified end,
                        // before or on the end date.
                        return (fileDate.isAfter(start) || fileDate.isEqual(start))
                                && (end == null || (fileDate.isBefore(end) || fileDate.isEqual(end)));
                    })
                    .peek(summary -> LOGGER.info("Found playable object at key '{}'", summary.getKey()))
                    .forEach(keys::add);

            listing = s3.listNextBatchOfObjects(listing);
        } while (!listing.getObjectSummaries().isEmpty());
        return keys;
    }
}
