package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

public class KinesisRecorder extends KinesisConnectorExecutorBase<byte[], byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecorder.class);

    private final KinesisConnectorConfiguration connectorConfiguration;
    private final AmazonS3 s3;

    public KinesisRecorder(VcrConfiguration vcrConfiguration, AmazonS3 s3, AWSCredentialsProvider credentialsProvider) {
        this.s3 = s3;

        Properties properties = new Properties();
        properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME,
                String.format(Locale.US, "kinesis-recorder-%s", vcrConfiguration.sourceStream));
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, vcrConfiguration.sourceStream);
        properties.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, vcrConfiguration.bucket);
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
                String.valueOf(vcrConfiguration.bufferSizeBytes));
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
                String.valueOf(vcrConfiguration.bufferTimeMillis));
        properties.setProperty(KinesisConnectorConfiguration.PROP_WORKER_ID, createWorkedId());

        // Check everything
        if (!s3.doesBucketExist(vcrConfiguration.bucket)) {
            throw new IllegalArgumentException("Requested bucket " + vcrConfiguration.bucket + " does not exist.");
        }

        connectorConfiguration = new KinesisConnectorConfiguration(properties, credentialsProvider);
        initialize(connectorConfiguration);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<byte[], byte[]> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new S3RecorderPipeline(s3), connectorConfiguration);
    }

    protected static String createWorkedId() {
        String workerId = String.format("localhost:%s", UUID.randomUUID());
        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            LOGGER.warn("Couldn't generate proper worker id - couldn't resolve host", e);
        }
        return workerId;
    }
}
