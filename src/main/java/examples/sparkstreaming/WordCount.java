package examples.sparkstreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.regex.Pattern;

public final class WordCount {
    static {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        /* Create the context with a 1 second batch size */
        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");

        /* Spark streaming context and set batch size to 1 second */
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        /* Listen for text data on a TCP connection */
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 50000);

        /* Split text into words */
        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)));

        /* Count each word once */
        JavaPairDStream<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));

        /* Aggregate counts */
        JavaPairDStream<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        counts.print();

        /* Start listening */
        ssc.checkpoint("/Users/ranjiti/temp");
        ssc.start();
        System.out.println("Start listening...");
        ssc.awaitTermination();
    }
}