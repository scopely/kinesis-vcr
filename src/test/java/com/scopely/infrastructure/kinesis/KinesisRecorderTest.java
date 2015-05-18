package com.scopely.infrastructure.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import org.fest.assertions.core.Condition;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class KinesisRecorderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecorderTest.class);

    private static AmazonDynamoDBClient dynamoDb;
    private static String kinesisStreamName;
    private static String bucketName;

    private static AmazonKinesis kinesis;
    private static AmazonS3 s3;
    private static AWSCredentialsProvider awsCredentialsProvider;

    @BeforeClass
    public static void prepareResources() throws Exception {
        awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        kinesis = new AmazonKinesisClient(awsCredentialsProvider);
        dynamoDb = new AmazonDynamoDBClient(awsCredentialsProvider);
        s3 = new AmazonS3Client(awsCredentialsProvider);

        kinesisStreamName = "krt-test-" + UUID.randomUUID().toString();
        bucketName = kinesisStreamName;

        kinesis.createStream(kinesisStreamName, 1);

        s3.createBucket(bucketName);

        while (!"ACTIVE".equals(kinesis.describeStream(kinesisStreamName).getStreamDescription().getStreamStatus())) {
            LOGGER.info("Waiting for stream…");
            Thread.sleep(1000);
        }

        while (!s3.doesBucketExist(bucketName)) {
            LOGGER.info("Waiting for bucket…");
            Thread.sleep(1000);
        }
    }

    @AfterClass
    public static void destroyResources() throws Exception {
        kinesis.deleteStream(kinesisStreamName);

        try {
            dynamoDb.deleteTable("kinesis-recorder-" + kinesisStreamName);
        } catch (Exception ignored) {

        }

        try {
            s3.deleteBucket(bucketName);
        } catch (Exception ignored) {

        }
    }

    @Before
    public void setUp() throws Exception {

    }

    @Test(expected = TimeoutException.class)
    public void testRecordingDoesntDie() throws Exception {
        VcrConfiguration configuration = new VcrConfiguration(kinesisStreamName, null, bucketName,
                1024 * 10, TimeUnit.SECONDS.toMillis(60));
        KinesisRecorder recorder = new KinesisRecorder(configuration, s3, awsCredentialsProvider);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> submit = executorService.submit(recorder);
        submit.get(10, TimeUnit.SECONDS);
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testRecordAndReplay() throws Exception {
        VcrConfiguration configuration = new VcrConfiguration(kinesisStreamName, kinesisStreamName, bucketName,
                1024 * 10, TimeUnit.SECONDS.toMillis(60));
        KinesisRecorder recorder = new KinesisRecorder(configuration, s3, awsCredentialsProvider);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(recorder);

        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName, ByteBuffer.wrap(new byte[40_000]), UUID.randomUUID().toString());

        Thread.sleep(TimeUnit.SECONDS.toMillis(45));

        List<S3ObjectSummary> objectSummaries = s3.listObjects(bucketName).getObjectSummaries();
        assertThat(objectSummaries).isNotEmpty();

        KinesisPlayer player = new KinesisPlayer(configuration, s3, kinesis);

        assertThat(objectSummaries.stream().flatMap(player::objectToPayloads).collect(Collectors.toList()))
                .are(new Condition<byte[]>() {
                    @Override
                    public boolean matches(byte[] value) {
                        return Arrays.equals(value, new byte[40_000]);
                    }
                });

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testFlushExpectedData() throws Exception {
        VcrConfiguration configuration = new VcrConfiguration(kinesisStreamName, kinesisStreamName, bucketName,
                1024 * 1024, TimeUnit.SECONDS.toMillis(10));

        AmazonS3 surveilledS3 = spy(s3);
        KinesisRecorder recorder = new KinesisRecorder(configuration, surveilledS3, awsCredentialsProvider);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(recorder);
        kinesis.putRecord(kinesisStreamName,
                ByteBuffer.wrap("String 1".getBytes(StandardCharsets.UTF_8)),
                UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName,
                ByteBuffer.wrap("String 2".getBytes(StandardCharsets.UTF_8)),
                UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName,
                ByteBuffer.wrap("String 3".getBytes(StandardCharsets.UTF_8)),
                UUID.randomUUID().toString());
        kinesis.putRecord(kinesisStreamName,
                ByteBuffer.wrap("String 4".getBytes(StandardCharsets.UTF_8)),
                UUID.randomUUID().toString());

        Thread.sleep(TimeUnit.SECONDS.toMillis(45));

        ArgumentCaptor<ByteArrayInputStream> baisCaptor = ArgumentCaptor.forClass(ByteArrayInputStream.class);

        verify(surveilledS3).putObject(anyString(), anyString(), baisCaptor.capture(), isNull(ObjectMetadata.class));

        assertThat(baisCaptor.getValue()).isNotNull();
        byte[] bytes = IOUtils.toByteArray(baisCaptor.getValue());
        assertThat(bytes).startsWith(Base64.getEncoder().encode("String 1".getBytes(StandardCharsets.UTF_8)));
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
}
