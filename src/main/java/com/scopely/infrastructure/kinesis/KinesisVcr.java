package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KinesisVcr {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisVcr.class);

    public static void main(String[] args) {
        VcrConfiguration vcrConfiguration = new VcrConfiguration(System.getenv());
        vcrConfiguration.validateConfiguration();

        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AmazonS3 s3 = new AmazonS3Client(credentialsProvider);
        AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider);

        if (args.length > 0 && ("play".equals(args[0]) || "estimate".equals(args[0]))) {

            if (vcrConfiguration.targetStream == null) {
                throw new IllegalArgumentException("Must specify a target stream for playback or estimation.");
            }

            if (args.length == 1) {
                throw new IllegalArgumentException("Must be called with at least two arguments: e.g., `kinesis-vcr play 2014-05-01T00:00:00 2015-05-01T00:00:00` " +
                        "or `kinesis-vcr play 2014-05-01T00:00:00`");
            }

            String startDateArg = args[1];

            LocalDateTime start = parseToLocalDateTime(startDateArg);

            if (start == null) {
                throw new IllegalArgumentException("Could not parse start date; should be formatted 2015-08-01 or 2015-08-01T12:12:00");
            }

            LocalDateTime end = null;
            if (args.length > 2) {
                end = parseToLocalDateTime(args[2]);

                if (end == null) {
                    throw new IllegalArgumentException("Could not parse end date; should be formatted 2015-08-01 or 2015-08-01T12:12:00");
                }
            }

            KinesisPlayer player = new KinesisPlayer(vcrConfiguration, s3, kinesis);

            if ("play".equals(args[0])) {
                replay(player, start, end, vcrConfiguration);
            } else {
                estimateReplayTime(player, start, end, vcrConfiguration);
            }
        } else {
            KinesisRecorder recorder = new KinesisRecorder(vcrConfiguration, s3, credentialsProvider);
            recorder.run();
        }
    }

    private static void estimateReplayTime(KinesisPlayer player, LocalDateTime start, LocalDateTime end, VcrConfiguration vcrConfiguration) {
        AtomicLong fileCount = new AtomicLong();
        long dataSizeInBytes = player
                .playableObjects(start, end)
                .doOnNext(ignore -> fileCount.incrementAndGet())
                .map(S3ObjectSummary::getSize)
                .reduce((lhs, rhs) -> lhs + rhs)
                .toBlocking()
                .first();

        // number of shards of the target stream
        int numberOfShards = player.getNumberOfShards();

        // all replayable data size in MB
        long dataSizeInMegaBytes = dataSizeInBytes / 1000 / 1000;

        // each shard can receive up to 1MB/s
        long estimatedTimeInMinutes = dataSizeInMegaBytes / numberOfShards / 60;

        // ceil time to biggest (reasonable) time unit
        String estimatedTimeHuman = minsToHumanReadableTimeSpan(estimatedTimeInMinutes);

        LOGGER.info("Target stream ({}) has {} shards", vcrConfiguration.targetStream, numberOfShards);
        LOGGER.info("It would take around {} to replay the data in the provided range, which has {} files and a total size of {} MB", estimatedTimeHuman, fileCount, dataSizeInMegaBytes);
    }

    private static void replay(KinesisPlayer player, LocalDateTime start, LocalDateTime end, VcrConfiguration vcrConfiguration) {
        AtomicInteger recordsCounter = new AtomicInteger();
        int count = player
                .play(start, end)
                .doOnNext(each -> System.out.print("Sent " + recordsCounter.incrementAndGet() + " records to kinesis\r"))
                .count()
                .toBlocking()
                .first();

        LOGGER.info("Wrote {} records to output Kinesis stream {}", count, vcrConfiguration.targetStream);
        System.exit(0);
    }

    private static LocalDateTime parseToLocalDateTime(String input) {
        LocalDateTime dateTime = null;
        try {
            dateTime = LocalDateTime.parse(input);
        } catch (DateTimeParseException ignored) {
            // no-op
        }

        try {
            dateTime = LocalDate.parse(input).atTime(0, 0);
        } catch (DateTimeParseException ignored) {
            // no-op
        }

        return dateTime;
    }

    /**
     * Rounds up the provided time in minutes to the biggest time unit (up to months)
     */
    private static String minsToHumanReadableTimeSpan(long timeInMinutes) {
        if (timeInMinutes < 60) {
            return String.format(Locale.US, "%d mins", timeInMinutes);
        }

        if (TimeUnit.MINUTES.toHours(timeInMinutes) < 24) {
            return String.format(Locale.US, "%d hours", TimeUnit.MINUTES.toHours(timeInMinutes));
        }

        long timeInDays = TimeUnit.MINUTES.toDays(timeInMinutes);
        if (timeInDays < 30) {
            return String.format(Locale.US, "%d days", timeInDays);
        }

        return String.format(Locale.US, "%d months", timeInDays / 30);
    }
}
