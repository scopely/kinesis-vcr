package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Properties;

public class KinesisRecorder extends KinesisConnectorExecutorBase<byte[], byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecorder.class);

    private final KinesisConnectorConfiguration connectorConfiguration;

    public KinesisRecorder(VcrConfiguration vcrConfiguration, AmazonS3 s3, AWSCredentialsProvider credentialsProvider) {

        Properties properties = new Properties();
        properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME,
                String.format(Locale.US, "kinesis-recorder-%s", vcrConfiguration.stream));
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, vcrConfiguration.stream);
        properties.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, vcrConfiguration.bucket);
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
                String.valueOf(vcrConfiguration.bufferSizeBytes));
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
                String.valueOf(vcrConfiguration.bufferTimeMillis));

        // Check everything
        if (!s3.doesBucketExist(vcrConfiguration.bucket)) {
            throw new IllegalArgumentException("Requested bucket " + vcrConfiguration.bucket + " does not exist.");
        }

        connectorConfiguration = new KinesisConnectorConfiguration(properties, credentialsProvider);
        initialize(connectorConfiguration);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<byte[], byte[]> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new S3RecorderPipeline(), connectorConfiguration);
    }
}
