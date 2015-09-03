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
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

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
            kinesis.describeStream(vcrConfiguration.targetStream);
        } catch (ResourceNotFoundException e) {
            LOGGER.error("Specified Kinesis stream '{}' not found", vcrConfiguration.targetStream);
            throw e;
        }
    }

    public void play(LocalDate start, @Nullable LocalDate end) {
        long count = playableObjects(start, end)
                .subscribeOn(Schedulers.io())
                .flatMap(this::objectToPayloads)
                .map(ByteBuffer::wrap)
                .map(payload -> kinesis.putRecord(vcrConfiguration.targetStream, payload, UUID.randomUUID().toString()))
                .retry((counter, throwable) -> {
                    LOGGER.error("Failed to put record in stream", throwable);
                    try {
                        Thread.sleep(5000l);
                    } catch (InterruptedException ignore) {
                    }
                    return counter < 3;
                })
                .doOnNext(result -> LOGGER.debug("Wrote record. Seq {}, shard {}", result.getSequenceNumber(), result.getShardId()))
                .count()
                .toBlocking()
                .first();

        LOGGER.info("Wrote {} records to output Kinesis stream {}", count, vcrConfiguration.targetStream);
    }

    Observable<byte[]> objectToPayloads(S3ObjectSummary summary) {
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

        return Observable.from(kinesisPayloads)
                         .map(b64Payload -> Base64.getDecoder().decode(b64Payload));
    }

    Observable<S3ObjectSummary> playableObjects(LocalDate start, @Nullable LocalDate end) {

        return Observable.create(new Observable.OnSubscribe<S3ObjectSummary>() {
            @Override
            public void call(Subscriber<? super S3ObjectSummary> subscriber) {
                String prefix = vcrConfiguration.sourceStream + "/";
                ObjectListing listing = s3.listObjects(vcrConfiguration.bucket, prefix);

                while (!subscriber.isUnsubscribed() && !listing.getObjectSummaries().isEmpty()) {
                    listing.getObjectSummaries()
                           .stream()
                           .filter(summary -> {
                               if (!summary.getKey().startsWith(vcrConfiguration.sourceStream)
                                       || summary.getKey().length() < vcrConfiguration.sourceStream.length() + 1 + "yyyy-MM-dd".length()) {
                                   return false;
                               }

                               String date = summary.getKey().substring(vcrConfiguration.sourceStream.length() + 1,
                                       vcrConfiguration.sourceStream.length() + 1 + "yyyy-MM-dd".length());

                               LocalDate fileDate = LocalDate.parse(date, S3RecorderPipeline.FORMATTER);

                               // We want objects with dates on or after the start, and, if there is a specified end,
                               // before or on the end date.
                               return (fileDate.isAfter(start) || fileDate.isEqual(start))
                                       && (end == null || (fileDate.isBefore(end) || fileDate.isEqual(end)));
                           })
                           .peek(summary -> {
                               LOGGER.info("Found playable object at key '{}'", summary.getKey());
                           })
                           .forEach(subscriber::onNext);

                    listing = s3.listNextBatchOfObjects(listing);
                }

                subscriber.onCompleted();
            }
        });
    }
}
