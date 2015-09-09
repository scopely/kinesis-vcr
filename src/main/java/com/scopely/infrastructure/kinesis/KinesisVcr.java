package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

public class KinesisVcr {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisVcr.class);

    public static void main(String[] args) {
        VcrConfiguration vcrConfiguration = new VcrConfiguration(System.getenv());
        vcrConfiguration.validateConfiguration();

        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AmazonS3 s3 = new AmazonS3Client(credentialsProvider);
        AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider);

        if (args.length > 0 && "play".equals(args[0])) {

            if (args.length == 1) {
                throw new IllegalArgumentException("Must be called with at least two arguments: e.g., `kinesis-vcr play 2014-05-01T00:00:00 2015-05-01T00:00:00` " +
                        "or `kinesis-vcr play 2014-05-01T00:00:00`");
            }

            String startDateArg = args[1];
            LocalDateTime start = LocalDateTime.parse(startDateArg);

            LocalDateTime end = null;
            if (args.length > 2) {
                end = LocalDateTime.parse(args[2]);
            }

            KinesisPlayer player = new KinesisPlayer(vcrConfiguration, s3, kinesis);
            AtomicInteger recordsCounter = new AtomicInteger();
            int count = player
                    .play(start, end)
                    .doOnNext(each -> System.out.print("Sent " + recordsCounter.incrementAndGet() + " records to kinesis\r"))
                    .count()
                    .toBlocking()
                    .first();

            LOGGER.info("Wrote {} records to output Kinesis stream {}", count, vcrConfiguration.targetStream);
        } else {
            KinesisRecorder recorder = new KinesisRecorder(vcrConfiguration, s3, credentialsProvider);
            recorder.run();
        }
    }
}
