package example.spark;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public final class WordCount {
    static {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        /* Give the app a name */
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");

        /* Establish a context */
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        /* Open the text file */
        JavaRDD<String> lines = ctx.textFile("count-words.txt");

        /* Transform the file into an array of words */
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)));

        /* Make a tuple for each word <Word, 1>" */
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s,1));

        /* Reduce step */
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        /* Get the result */
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        /* Stop the context */
        ctx.stop();
    }
}