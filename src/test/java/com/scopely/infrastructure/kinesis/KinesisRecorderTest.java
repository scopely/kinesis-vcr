package com.scopely.infrastructure.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.IOUtils;
import org.fest.assertions.core.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class KinesisRecorderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecorderTest.class);

    private AmazonDynamoDB dynamoDb;
    private String kinesisStreamName;
    private String bucketName;
    private AmazonS3 s3;
    private AmazonKinesis kinesis;
    private AWSCredentialsProvider awsCredentialsProvider;

    @Before
    public void setUp() throws Exception {
        awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        kinesis = new AmazonKinesisClient(awsCredentialsProvider);
        dynamoDb = new AmazonDynamoDBClient(awsCredentialsProvider);

        kinesisStreamName = "krt-test-" + UUID.randomUUID().toString();

        kinesis.createStream(kinesisStreamName, 1);

        while (!"ACTIVE".equals(kinesis.describeStream(kinesisStreamName).getStreamDescription().getStreamStatus())) {
            LOGGER.info("Waiting for stream…");
            Thread.sleep(1000);
        }

        bucketName = kinesisStreamName + UUID.randomUUID().toString().substring(0, 4);
        s3 = new AmazonS3Client(awsCredentialsProvider);
        s3.createBucket(bucketName);
        while (!s3.doesBucketExist(bucketName)) {
            LOGGER.info("Waiting for bucket {} …", bucketName);
            Thread.sleep(1000);
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            s3.listObjects(bucketName).getObjectSummaries().forEach(summary -> {
                s3.deleteObject(summary.getBucketName(), summary.getKey());
            });
        } catch (Exception e) {
            LOGGER.warn("Failed to delete objects from s3. Ignoring on tearDown: ", e);
        }

        try {
            s3.deleteBucket(bucketName);
        } catch (Exception e) {
            LOGGER.warn("Failed to delete bucket " + bucketName + ". Ignoring on tearDown: ", e);
        }

        try {
            kinesis.deleteStream(kinesisStreamName);
        } catch (AmazonClientException e) {
            LOGGER.warn("Failed to delete stream " + kinesisStreamName + ". Ignoring on tearDown: ", e);
        }

        try {
            dynamoDb.deleteTable("kinesis-recorder-" + kinesisStreamName);
        } catch (Exception e) {
            LOGGER.warn("Failed to delete kinesis leasing table kinesis-recorder-" + kinesisStreamName + ". Ignoring on tearDown: ", e);
        }
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

        KinesisPlayer player = new KinesisPlayer(configuration, s3, kinesis);

        Observable<byte[]> bytesObservable = player
                .playableObjects(LocalDateTime.now(), LocalDateTime.now().plusDays(1))
                .flatMap(player::objectToPayloads);

        TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        bytesObservable.subscribe(testSubscriber);


        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();

        List<byte[]> result = testSubscriber.getOnNextEvents();
        assertThat(result).isNotEmpty();
        assertThat(result).are(new Condition<byte[]>() {
            @Override
            public boolean matches(byte[] value) {
                return Arrays.equals(value, new byte[40_000]);
            }
        });

        executorService.shutdown();
        recorder.stop();
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

        verify(surveilledS3).putObject(anyString(), anyString(), baisCaptor.capture(), any(ObjectMetadata.class));

        assertThat(baisCaptor.getValue()).isNotNull();
        baisCaptor.getValue().reset();
        byte[] bytes = IOUtils.toByteArray(baisCaptor.getValue());
        assertThat(bytes).startsWith(Base64.getEncoder().encode("String 1".getBytes(StandardCharsets.UTF_8)));
        recorder.stop();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
}
