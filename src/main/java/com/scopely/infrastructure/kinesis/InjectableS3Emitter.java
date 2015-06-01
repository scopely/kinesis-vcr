package com.scopely.infrastructure.kinesis;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Based on {@link com.amazonaws.services.kinesis.connectors.s3.S3Emitter}, but allows for the injection of a specific
 * instance of {@link AmazonS3}, making testing more straightforward.
 */
public class InjectableS3Emitter implements IEmitter<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3RecorderPipeline.class);
    private final KinesisConnectorConfiguration configuration;
    private final AmazonS3 s3;
    protected final String s3Bucket;

    public InjectableS3Emitter(KinesisConnectorConfiguration configuration, AmazonS3 amazonS3) {
        this.configuration = configuration;
        s3Bucket = configuration.S3_BUCKET;
        this.s3 = amazonS3;
    }

    protected String getS3FileName(String firstSeq, String lastSeq) {
        return String.format(Locale.US, "%s/%s/%s-%s",
                configuration.KINESIS_INPUT_STREAM,
                Clock.systemUTC().instant().atOffset(ZoneOffset.UTC).format(S3RecorderPipeline.FORMATTER),
                firstSeq, lastSeq);
    }
    protected String getS3URI(String s3FileName) {
        return "s3://" + s3Bucket + "/" + s3FileName;
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        List<byte[]> records = buffer.getRecords();
        // Write all of the records to a compressed output stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] record : records) {
            try {
                baos.write(record);
            } catch (Exception e) {
                LOGGER.error("Error writing record to output stream. Failing this emit attempt. Record: "
                                + Arrays.toString(record),
                        e);
                return buffer.getRecords();
            }
        }
        // Get the Amazon S3 filename
        String s3FileName = getS3FileName(buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());
        String s3URI = getS3URI(s3FileName);
        try {
            ByteArrayInputStream object = new ByteArrayInputStream(baos.toByteArray());
            LOGGER.debug("Starting upload of file " + s3URI + " to Amazon S3 containing " + records.size() + " records.");
            s3.putObject(s3Bucket, s3FileName, object, null);
            LOGGER.info("Successfully emitted " + buffer.getRecords().size() + " records to Amazon S3 in " + s3URI);
            return Collections.emptyList();
        } catch (Exception e) {
            LOGGER.error("Caught exception when uploading file " + s3URI + "to Amazon S3. Failing this emit attempt.", e);
            return buffer.getRecords();
        }
    }

    @Override
    public void fail(List<byte[]> records) {
        for (byte[] record : records) {
            LOGGER.error("Record failed: " + Arrays.toString(record));
        }
    }

    @Override
    public void shutdown() {
        // don't shut down the S3 client, since it might be shared.
    }
}
