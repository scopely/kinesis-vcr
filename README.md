# kinesis-vcr
Rewind and (later) replay live Kinesis streams

![](https://38.media.tumblr.com/9a99c0a5241819a17ed1ab4c3440f755/tumblr_n11yb6eXru1toe3mso1_400.gif)

The VCR uses the [Amazon Kinesis Connectors](https://github.com/awslabs/amazon-kinesis-connectors) and [Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client) internally, meaning it automatically uses a DynamoDB table for lease management.

## Running locally?

### Build
```
./gradlew installDist
```

### Run

Record a stream:
```
VCR_SOURCE_STREAM_NAME=my-important-data 
VCR_BUCKET_NAME=dev-kinesis-backups 
./build/install/kinesis-vcr/bin/kinesis-vcr record
```

This will write to files like `s3://dev-kinesis-backups/my-important-data/2015-05-12/49545259625339426540861503851695364890964474172851355682-49545259625339426540861503967844121936259603873071628322`, where the date is the date that the chunk is written to S3 and the long numbers are the start and end sequence numbers of this chunk of stream data.

Replay a stream:
```
VCR_SOURCE_STREAM_NAME=my-important-data
VCR_TARGET_STREAM_NAME=recovery-stream
VCR_BUCKET_NAME=dev-kinesis-backups 
./build/install/kinesis-vcr/bin/kinesis-vcr play 2014-05-01 2014-05-10
```

This will take a recording of the stream `my-important-data` recorded in the bucket `dev-kinesis-backups` and replay records for the date range 2014-05-01 to 2014-05-10 (exclusive of the end) onto the stream `recovery-stream`. If only one day of data should be play replayed use:

```
./build/install/kinesis-vcr/bin/kinesis-vcr play 2014-05-01
```

Replay can also specify times, for a narrow playback range:

```
./build/install/kinesis-vcr/bin/kinesis-vcr play 2014-05-01T12:00:00 2014-05-03:13:45:00
```

Specified times are always interpreted as UTC. 

Prior to replaying a stream, the VCR can provide an estimated playing time, which is simply the size of the input dataset divided by the write throughput possible for the target stream. `kinesis-vcr estimate` takes the same parameters as `kinesis-vcr play`:

```bash
$ kinesis-vcr estimate 2014-05-01T12:00:00 2014-05-03:13:45:00
[main] INFO com.scopely.infrastructure.kinesis.KinesisVcr - Target stream (kinesis-playback-test) has 2 shards
[main] INFO com.scopely.infrastructure.kinesis.KinesisVcr - It would take around 50 mins to replay the data in the provided range, which has 341 files and a total size of 6038 MB
```

## Running somewhere else?

### JAR distribution

#### Build
Make a fat JAR:
```
./gradlew shadowJar
```
Your JAR will be in `build/libs/kinesis-vcr-1.0.4-SNAPSHOT-all.jar`.

#### Run
```
java -jar kinesis-vcr-1.0.4-SNAPSHOT-all.jar record
```

### Debian

#### Build
Make a Debian package:
```
./gradlew buildDeb
```

Your package will be in `build/distributions/kinesis-vcr_1.0.0_all.deb`.

#### Run
Install the package. The script `kinesis-vcr` will be installed to `/opt/scopely/kinesis-vcr/bin/kinesis-vcr`.

Invoke it like you would locally, _e.g._:

```
VCR_SOURCE_STREAM_NAME=my-important-data 
VCR_BUCKET_NAME=dev-kinesis-backups 
/opt/scopely/kinesis-vcr/bin/kinesis-vcr record
```

## Configuration and options

The VCR assumes that it can find AWS credentials in the default locations, according to the rules of `DefaultAWSCredentialsProviderChain`:
 
* Environment Variables - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY`
* Java System Properties - `aws.accessKeyId` and `aws.secretKey`
* Credential profiles file at the default location (`~/.aws/credentials`) shared by all AWS SDKs and the AWS CLI
* Instance profile credentials delivered through the Amazon EC2 metadata service

In addition to the required parameters `VCR_SOURCE_STREAM_NAME` and `VCR_BUCKET_NAME`, the VCR also respects `VCR_BUFFER_SIZE_BYTES`, controlling how much data to buffer before writing to S3, and `VCR_BUFFER_TIME_MILLIS`, controlling how long to buffer data before writing to S3.

When playing from S3, `VCR_TARGET_STREAM_NAME` must also be specified.

More bells and whistles are sure to come.

## Format

The VCR writes records to S3 as newline-delimited Base64 -- each record on the input stream is written to a line in the output file. On playback, it just Base64-decodes each line and emits it on the target stream. As such, this tool is completely agnostic to the format of records on the wire.
