package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;

public class KinesisVcr {

    public static void main(String[] args) {
        VcrConfiguration vcrConfiguration = new VcrConfiguration(System.getenv());
        vcrConfiguration.validateConfiguration();

        if (args.length > 0 && "play".equals(args[0])) {
            KinesisPlayer player = new KinesisPlayer(vcrConfiguration);
            player.run();
        } else {
            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            KinesisRecorder recorder = new KinesisRecorder(vcrConfiguration,
                    new AmazonS3Client(credentialsProvider),
                    new AmazonKinesisClient(credentialsProvider));
            recorder.run();
        }
    }
}
