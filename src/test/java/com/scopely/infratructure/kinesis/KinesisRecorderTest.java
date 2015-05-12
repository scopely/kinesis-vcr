package com.scopely.infratructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
import com.scopely.infrastructure.kinesis.KinesisRecorder;
import com.scopely.infrastructure.kinesis.VcrConfiguration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisRecorderTest {
    private KinesisRecorder recorder;

    private static String kinesisStreamName;
    private static String bucketName;

    private static AmazonKinesis kinesis;
    private AmazonS3 s3;
    private static AWSCredentialsProvider awsCredentialsProvider;

    @BeforeClass
    public static void prepareResources() throws Exception {
        awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        kinesis = new AmazonKinesisClient(awsCredentialsProvider);

        kinesisStreamName = "krt-test-" + UUID.randomUUID().toString();
        bucketName = kinesisStreamName;

        kinesis.createStream(kinesisStreamName, 1);
    }

    @AfterClass
    public static void destroyResources() throws Exception {
        kinesis.deleteStream(kinesisStreamName);
    }

    @Before
    public void setUp() throws Exception {
        s3 = mock(AmazonS3.class);
        when(s3.doesBucketExist(anyString())).thenReturn(true);

        VcrConfiguration configuration = new VcrConfiguration(kinesisStreamName, bucketName,
                1024 * 1024, TimeUnit.SECONDS.toMillis(60));
        recorder = new KinesisRecorder(configuration, s3, kinesis);
    }

    @Test(expected = TimeoutException.class)
    public void testRecordingDoesntDie() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> submit = executorService.submit(recorder);
        submit.get(10, TimeUnit.SECONDS);

    }
}
