package cl.tarea4;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

public class App
{
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {

        String brokers = args[0];
        String topics = args[1];

        // Create context with 2 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("App");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        /*
        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
        */

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createStream(
                jssc,
                "localhost:2181",
                "group.id",
                ImmutableMap.of("testTopic", 1)
        );


        // Get the lines, count the lines in this case
        messages.foreach(new Function<JavaPairRDD<String, String>, Void>(){
            @Override
            public Void call(JavaPairRDD<String, String> pairs) throws Exception {
                System.out.println("RDD Count: " + String.valueOf(pairs.count()));
                pairs.mapValues(new Function<String, Object>() {
                    @Override
                    public Object call(String s) throws Exception {
                        String[] parts = s.split(" ");
                        for(String a : parts){
                            System.out.println(a);
                        }
                        return null;
                    }
                });
                System.out.println("--------------------------------||--------------------");
                return null;
            }
        });

        //get the <Key, Latitude> pairs
        JavaPairDStream<String,Double> keyLat = messages.mapToPair(new PairFunction<Tuple2<String, String>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, String> pair) throws Exception {
                String[] parts = pair._2.split(" ");
                return new Tuple2<String, Double>(pair._1,Double.valueOf(parts[3]));
            }
        });


        /*
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.print();
        */


        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}