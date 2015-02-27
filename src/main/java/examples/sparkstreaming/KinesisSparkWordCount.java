package examples.sparkstreaming;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class KinesisSparkWordCount {
    private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");
    static {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    public static void main(String[] args) {
        String streamName = "WordStream";
        String endpointUrl = "https://kinesis.us-east-1.amazonaws.com";

        Duration batchInterval = new Duration(5000);

        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonKinesisClient kinesisClient = new AmazonKinesisClient(credentials);

        kinesisClient.setEndpoint(endpointUrl);

        /* Determine the number of shards from the stream */
        int numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();

        /* In this example, we're going to create 1 Kinesis Worker/Receiver/DStream for each shard */
        int numStreams = numShards;

        /* Setup the Spark config. */
        SparkConf sparkConfig = new SparkConf().setAppName("KinesisWordCount").setMaster("local[2]");

        /* Kinesis checkpoint interval.  Same as batchInterval for this example. */
        Duration checkpointInterval = batchInterval;

        /* Setup the StreamingContext */
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        /* Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards */
        JavaDStream<byte[]> kinesisStream = KinesisUtils.createStream(jssc, streamName, endpointUrl, checkpointInterval,
                InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2());

        JavaDStream<String> words = kinesisStream.flatMap(line -> Arrays.asList(WORD_SEPARATOR.split(new String(line))));

        JavaPairDStream<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s,1));

        JavaPairDStream<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        /* Print the first 10 wordCounts */
        counts.print();

        /* Start the streaming context and await termination */
        jssc.start();

        jssc.awaitTermination();

    }
}
