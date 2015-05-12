package com.scopely.infrastructure.kinesis;

import com.beust.jcommander.Parameter;

import java.util.concurrent.TimeUnit;

public class VcrConfiguration {
    @Parameter(required = true, names = "--stream")
    String stream;

    @Parameter(required = true, names = "--bucket")
    String bucket;

//    @Parameter(required = false, names = "--prefix-pattern")
//    String s3KeyPrefix = "stream_%s/shard_%s/%s/start=%s,end=%s";

    @Parameter(required = false, names = "--buffer-size-bytes")
    long bufferSizeBytes = 1_024l * 1_024l * 100l;

    @Parameter(required = false, names = "--buffer-time-millis")
    long bufferTimeMillis = TimeUnit.SECONDS.toMillis(60);

    @Parameter(help = true, names = {"-h", "--help"})
    boolean help;

}
