package com.scopely.infrastructure.kinesis;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class VcrConfiguration {
    String sourceStream;

    String targetStream;

    String bucket;

    long bufferSizeBytes = 1_024l * 1_024l * 100l;

    long bufferTimeMillis = TimeUnit.SECONDS.toMillis(60);

    public VcrConfiguration(Map<String, String> getenv) {
        sourceStream = getenv.get("VCR_SOURCE_STREAM_NAME");
        targetStream = getenv.get("VCR_TARGET_STREAM_NAME");
        bucket = getenv.get("VCR_BUCKET_NAME");
        bufferSizeBytes = Long.parseLong(getenv.getOrDefault("VCR_BUFFER_SIZE_BYTES", String.valueOf(bufferSizeBytes)));
        bufferTimeMillis = Long.parseLong(getenv.getOrDefault("VCR_BUFFER_TIME_MILLIS", String.valueOf(bufferTimeMillis)));
    }

    public VcrConfiguration(String sourceStream,
                            String targetStream,
                            String bucket,
                            long bufferSizeBytes,
                            long bufferTimeMillis) {
        this.sourceStream = sourceStream;
        this.bucket = bucket;
        this.bufferSizeBytes = bufferSizeBytes;
        this.bufferTimeMillis = bufferTimeMillis;
    }

    public void validateConfiguration() {
        if (sourceStream == null &&  targetStream == null) {
            throw new IllegalArgumentException("VCR_SOURCE_STREAM_NAME or VCR_TARGET_STREAM_NAME must be set");
        }

        if (bucket == null) {
            throw new IllegalArgumentException("VCR_BUCKET_NAME must be set");
        }
    }
}
