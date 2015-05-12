package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.util.Locale;
import java.util.Properties;

public class KinesisRecorder extends KinesisConnectorExecutorBase<byte[], byte[]> {
    private final KinesisConnectorConfiguration connectorConfiguration;

    public KinesisRecorder(VcrConfiguration vcrConfiguration) {

        Properties properties = new Properties();
        properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME,
                String.format(Locale.US, "kinesis-recorder-%s", vcrConfiguration.stream));
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, vcrConfiguration.stream);
        properties.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, vcrConfiguration.bucket);
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
                String.valueOf(vcrConfiguration.bufferSizeBytes));
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
                String.valueOf(vcrConfiguration.bufferTimeMillis));

        AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        // Check everything
        AmazonS3 amazonS3Client = new AmazonS3Client(awsCredentialsProvider);
        if (!amazonS3Client.doesBucketExist(vcrConfiguration.bucket)) {
            throw new IllegalArgumentException("Requested bucket " + vcrConfiguration.bucket + " does not exist.");
        }

        connectorConfiguration = new KinesisConnectorConfiguration(properties, awsCredentialsProvider);
        initialize(connectorConfiguration);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<byte[], byte[]> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new S3RecorderPipeline(), connectorConfiguration);
    }
}
