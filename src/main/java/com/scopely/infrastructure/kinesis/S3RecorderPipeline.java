package com.scopely.infrastructure.kinesis;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;

public class S3RecorderPipeline implements IKinesisConnectorPipeline<byte[], byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3RecorderPipeline.class);

    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final AmazonS3 s3;

    public S3RecorderPipeline(AmazonS3 amazonS3) {
        s3 = amazonS3;
    }

    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        return new InjectableS3Emitter(configuration, s3);
    }

    @Override
    public IBuffer<byte[]> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<>(configuration);
    }

    @Override
    public ITransformerBase<byte[], byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new ITransformer<byte[], byte[]>() {
            @Override
            public byte[] toClass(Record record) throws IOException {
                return record.getData().array();
            }

            @Override
            public byte[] fromClass(byte[] record) throws IOException {
                byte[] encoded = Base64.getEncoder().encode(record);
                byte[] expanded = Arrays.copyOf(encoded, encoded.length + 1);
                expanded[encoded.length] = '\n';
                return expanded;
            }
        };
    }

    @Override
    public IFilter<byte[]> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<>();
    }
}
