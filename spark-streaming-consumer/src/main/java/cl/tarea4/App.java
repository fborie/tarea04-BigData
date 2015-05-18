package cl.tarea4;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
import redis.clients.jedis.Jedis;
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
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

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

        /*
        // Get the lines, split them into words, count the words and print
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
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        */

        JavaPairDStream<String, String> latitude = messages.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> s) {
                        return new Tuple2<>(s._1(), s._2().split(" ")[0]);
                    }
                });

        JavaPairDStream<String, String> longitude = messages.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> s) {
                        return new Tuple2<>(s._1(), s._2().split(" ")[1]);
                    }
                });

        JavaPairDStream<String, String> altitud = messages.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> s) {
                        return new Tuple2<>(s._1(), s._2().split(" ")[1]);
                    }
                });

        JavaPairDStream<String, String> speed = messages.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> s) {
                        return new Tuple2<>(s._1(), s._2().split(" ")[1]);
                    }
                });

        JavaPairDStream<String, Integer> planeCount = messages.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> flightInfo) {
                    return new Tuple2<String, Integer>(flightInfo._2().split(" ")[4], 1);
                }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        //Saving to Jedis
        planeCount.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            public Void call(JavaPairRDD<String, Integer> rdd) {
                Jedis jedis = new Jedis("localhost");
                for (Tuple2<String, Integer> t: rdd.collect()) {
                    jedis.set(t._1(), String.valueOf(t._2()) );
                }
                return null;
            }
        });

        //wordCounts.print();
        //latitude.print();
        planeCount.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}