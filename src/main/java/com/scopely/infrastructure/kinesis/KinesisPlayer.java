package com.scopely.infrastructure.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import rx.Observable;
import rx.Subscriber;

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

    public Observable<PutRecordResult> play(LocalDate start, @Nullable LocalDate end) {
        return playableObjects(start, end)
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
                .doOnNext(result -> LOGGER.debug("Wrote record. Seq {}, shard {}", result.getSequenceNumber(), result.getShardId()));
    }

    public Observable<byte[]> objectToPayloads(S3ObjectSummary summary) {
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


    /**
     * Returns an observable that emits all the S3 objects between the provided start and end date.
     */
    public Observable<S3ObjectSummary> playableObjects(@NotNull LocalDate start, @Nullable LocalDate end) {
        if (end != null && start.isAfter(end)) {
            throw new IllegalArgumentException("startDate > endDate");
        }
        if (end == null) {
            end = start;
        }

        final LocalDate finalEnd = end;
        return Observable.create(new Observable.OnSubscribe<Observable<S3ObjectSummary>>() {
            @Override
            public void call(Subscriber<? super Observable<S3ObjectSummary>> subscriber) {
                // get all S3 objects for each date between start and end
                for (LocalDate currentDate = start; !finalEnd.isBefore(currentDate); currentDate = currentDate.plus(1, ChronoUnit.DAYS)) {
                    subscriber.onNext(playableObjects(currentDate));
                }
                subscriber.onCompleted();
            }
        }).flatMap(x -> x);
    }

    /**
     * Returns all objects saved under the provided date folder
     */
    private Observable<S3ObjectSummary> playableObjects(LocalDate date) {

        return Observable.create(new Observable.OnSubscribe<S3ObjectSummary>() {
            @Override
            public void call(Subscriber<? super S3ObjectSummary> subscriber) {
                // list objects under the currentDate folder
                String prefix = vcrConfiguration.sourceStream + "/" + date;
                ObjectListing listing = s3.listObjects(vcrConfiguration.bucket, prefix);

                while (!subscriber.isUnsubscribed() && !listing.getObjectSummaries().isEmpty()) {
                    listing.getObjectSummaries()
                           .stream()
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
