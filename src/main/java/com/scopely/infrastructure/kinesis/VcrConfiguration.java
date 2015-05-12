package com.scopely.infrastructure.kinesis;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class VcrConfiguration {
    String stream;

    String bucket;

    long bufferSizeBytes = 1_024l * 1_024l * 100l;

    long bufferTimeMillis = TimeUnit.SECONDS.toMillis(60);

    public VcrConfiguration(Map<String, String> getenv) {
        stream = getenv.get("VCR_STREAM_NAME");
        bucket = getenv.get("VCR_BUCKET_NAME");
        bufferSizeBytes = Long.parseLong(getenv.getOrDefault("VCR_BUFFER_SIZE_BYTES", String.valueOf(bufferSizeBytes)));
        bufferTimeMillis = Long.parseLong(getenv.getOrDefault("VCR_BUFFER_TIME_MILLIS", String.valueOf(bufferTimeMillis)));
    }

    public VcrConfiguration(String stream, String bucket, long bufferSizeBytes, long bufferTimeMillis) {
        this.stream = stream;
        this.bucket = bucket;
        this.bufferSizeBytes = bufferSizeBytes;
        this.bufferTimeMillis = bufferTimeMillis;
    }

    public void validateConfiguration() {
        if (stream == null) {
            throw new IllegalArgumentException("VCR_STREAM_NAME must be set");
        }

        if (bucket == null) {
            throw new IllegalArgumentException("VCR_BUCKET_NAME must be set");
        }
    }
}
