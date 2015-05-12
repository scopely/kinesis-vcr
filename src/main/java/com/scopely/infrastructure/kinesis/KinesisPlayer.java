package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class KinesisPlayer implements Runnable {

    public KinesisPlayer(VcrConfiguration vcrConfiguration) {
        AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        // Check everything
        AmazonS3 amazonS3Client = new AmazonS3Client(awsCredentialsProvider);
        if (!amazonS3Client.doesBucketExist(vcrConfiguration.bucket)) {
            throw new IllegalArgumentException("Requested bucket " + vcrConfiguration.bucket + " does not exist.");
        }

        AmazonKinesis amazonKinesis = new AmazonKinesisClient(awsCredentialsProvider);

        //noinspection CaughtExceptionImmediatelyRethrown
        try {
            amazonKinesis.describeStream(vcrConfiguration.stream);
        } catch (ResourceNotFoundException e) {
            throw e;
        }
    }

    @Override
    public void run() {
        //
    }
}
