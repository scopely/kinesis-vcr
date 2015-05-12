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
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;
import com.amazonaws.services.kinesis.model.Record;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class S3RecorderPipeline implements IKinesisConnectorPipeline<byte[], byte[]> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");


    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        return new S3Emitter(configuration) {
            @Override
            protected String getS3FileName(String firstSeq, String lastSeq) {
                return String.format(Locale.US, "%s/%s/%s-%s",
                        configuration.KINESIS_INPUT_STREAM,
                        Clock.systemUTC().instant().atOffset(ZoneOffset.UTC).format(FORMATTER),
                        firstSeq, lastSeq);
            }
        };
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
                return record;
            }
        };
    }

    @Override
    public IFilter<byte[]> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<>();
    }
}
