package com.scopely.infrastructure.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import static java.util.stream.Collectors.toList;

public class KinesisPlayer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisPlayer.class);

    private static final int MAX_KINESIS_BATCH_SIZE = 500;
    private static final int MAX_KINESIS_BATCH_WEIGHT = 1_000_000;
    private static final int KINESIS_PUT_BATCH_RETRIES_TIMEOUT = 30;

    private final VcrConfiguration vcrConfiguration;
    private final AmazonS3 s3;
    private final AmazonKinesis kinesis;

    private final ExecutorService kinesisWriter = Executors.newFixedThreadPool(10);
    private final int numberOfShards;

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
            DescribeStreamResult describeStreamResult = kinesis.describeStream(vcrConfiguration.targetStream);
            numberOfShards = (int) describeStreamResult
                    .getStreamDescription()
                    .getShards()
                    .stream()
                    /* only take into account open shards */
                    .filter(shard -> shard.getSequenceNumberRange().getEndingSequenceNumber() == null)
                    .count();
        } catch (ResourceNotFoundException e) {
            LOGGER.error("Specified Kinesis stream '{}' not found", vcrConfiguration.targetStream);
            throw e;
        }
    }

    public Observable<PutRecordsResultEntry> play(LocalDateTime start, @Nullable LocalDateTime end) {
        return playableObjects(start, end)
                .onBackpressureBuffer()
                .observeOn(Schedulers.io())
                .flatMap(this::objectToPayloads)
                .map(ByteBuffer::wrap)
                .lift(new OperatorBufferKinesisBatch(MAX_KINESIS_BATCH_SIZE, MAX_KINESIS_BATCH_WEIGHT))
                .onBackpressureBuffer()
                .map(byteBuffers -> byteBuffers.stream()
                        .map(buffer -> new PutRecordsRequestEntry()
                                .withData(buffer)
                                .withPartitionKey(UUID.randomUUID().toString()))
                        .collect(toList()))
                .map(entries -> new PutRecordsRequest()
                        .withStreamName(vcrConfiguration.targetStream)
                        .withRecords(entries))
                .observeOn(Schedulers.io())
                .flatMap(putRecordsRequest -> Observable.create((Observable.OnSubscribe<List<PutRecordsResult>>) os -> {
                    os.onStart();
                    kinesisWriter.submit(() -> {
                        os.onNext(putWithRetry(putRecordsRequest).orElse(Collections.<PutRecordsResult>emptyList()));
                        os.onCompleted();
                    });
                }))
                .flatMap(Observable::from)
                .flatMap(putRecordsResult -> Observable.from(putRecordsResult.getRecords()))
                .doOnNext(result -> LOGGER.debug("Wrote record. Seq {}, shard {}", result.getSequenceNumber(), result.getShardId()));
    }

    /**
     * Tries to send the provided request to kinesis, retrying records that failed to be processed
     */
    private Optional<List<PutRecordsResult>> putWithRetry(PutRecordsRequest putRecordsRequest) {
        long totalSize = putRecordsRequest.getRecords().stream().mapToLong(record -> record.getData().limit()).sum();
        LOGGER.info("Sending {} records ({} bytes)", putRecordsRequest.getRecords().size(), totalSize);

        try {
            List<PutRecordsResult> resultSetList = new ArrayList<>();
            return ExponentialBackoffRunner.run(() -> {
                        PutRecordsResult putRecordsResult = kinesis.putRecords(putRecordsRequest);
                        resultSetList.add(putRecordsResult);
                        if (putRecordsResult.getFailedRecordCount() > 0) {
                            List<PutRecordsRequestEntry> entriesForRetry = new ArrayList<>();
                            for (int i = 0; i < putRecordsResult.getRecords().size(); i++) {
                                PutRecordsResultEntry resultEntry = putRecordsResult.getRecords().get(i);
                                PutRecordsRequestEntry requestEntry = putRecordsRequest.getRecords().get(i);
                                if (resultEntry.getErrorCode() != null) {
                                    entriesForRetry.add(requestEntry);
                                }
                            }
                            putRecordsRequest.withRecords(entriesForRetry);
                            if (entriesForRetry.size() > 0) {
                                LOGGER.warn("Retrying {} records", entriesForRetry.size());
                                throw new PartialFailureException();
                            }
                        }
                        return resultSetList;
                    },
                    throwable -> throwable instanceof ProvisionedThroughputExceededException
                            || throwable instanceof AmazonClientException
                            || throwable instanceof PartialFailureException,
                    TimeUnit.SECONDS.toMillis(KINESIS_PUT_BATCH_RETRIES_TIMEOUT));
        } catch (Throwable throwable) {
            throw new RuntimeException("Unhandled exception from Kinesis put", throwable);
        }
    }

    private class PartialFailureException extends RuntimeException {
    }

    public Observable<byte[]> objectToPayloads(S3ObjectSummary summary) {
        LOGGER.info("Found playable object from {} at key '{}'", summary.getLastModified(), summary.getKey());
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
                .map(b64Payload -> Base64.getDecoder().decode(b64Payload)).subscribeOn(Schedulers.io());
    }


    /**
     * Returns an observable that emits all the S3 objects between the provided start and end date.
     */
    public Observable<S3ObjectSummary> playableObjects(@NotNull LocalDateTime start, @Nullable LocalDateTime end) {
        if (end != null && start.isAfter(end)) {
            throw new IllegalArgumentException("startDate > endDate");
        }
        if (end == null) {
            // Since end was not provided, let's assume we want data for the provided start day.
            // That means we must match s3 objects between start and start + 1 day.
            // We subtract 1 second in order to avoid checking for files in the next day folder.
            end = start.plusDays(1).minusSeconds(1);
        }


        final LocalDateTime finalEnd = end;

        Predicate<Date> dateFilter = date -> {
            return start.atOffset(ZoneOffset.UTC).toEpochSecond() < date.getTime() / 1000
                    && finalEnd.atOffset(ZoneOffset.UTC).toEpochSecond() > date.getTime() / 1000;
        };


        return Observable.create(new Observable.OnSubscribe<Observable<S3ObjectSummary>>() {
            @Override
            public void call(Subscriber<? super Observable<S3ObjectSummary>> subscriber) {
                // get all S3 objects for each date between start and end
                for (LocalDateTime currentDate = start; !finalEnd.isBefore(currentDate); currentDate = currentDate.plus(1, ChronoUnit.DAYS)) {
                    subscriber.onNext(playableObjects(currentDate));
                }
                subscriber.onCompleted();
            }
        })
                .subscribeOn(Schedulers.io())
                .flatMap(x -> x)
                .filter(x -> dateFilter.test(x.getLastModified()))
                .onBackpressureBuffer();
    }

    /**
     * Returns all objects saved under the provided date folder
     */
    private Observable<S3ObjectSummary> playableObjects(LocalDateTime date) {

        return Observable.create(new Observable.OnSubscribe<Observable<S3ObjectSummary>>() {
            @Override
            public void call(Subscriber<? super Observable<S3ObjectSummary>> subscriber) {
                // list objects under the currentDate folder
                String prefix = vcrConfiguration.sourceStream + "/" + date.format(S3RecorderPipeline.FORMATTER);
                ObjectListing listing = s3.listObjects(vcrConfiguration.bucket, prefix);

                while (!subscriber.isUnsubscribed() && !listing.getObjectSummaries().isEmpty()) {
                    subscriber.onNext(Observable.from(listing.getObjectSummaries()));
                    // Retry on everything!
                    listing = ExponentialBackoffRunner.run(() -> s3.listNextBatchOfObjects(listing),
                            t -> true, TimeUnit.MINUTES.toMillis(2));
                }

                subscriber.onCompleted();
            }
        }).onBackpressureBuffer().flatMap(x -> x).subscribeOn(Schedulers.io());
    }

    /**
     * @return number of shards of the target stream
     */
    public int getNumberOfShards() {
        return numberOfShards;
    }
}
