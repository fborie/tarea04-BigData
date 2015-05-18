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
import redis.clients.util.SafeEncoder;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

        */

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

        /*
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
        */


        JavaPairDStream<String, String> allInfo = messages.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> s) {
                        return new Tuple2<>(s._1(), s._2().split(" ")[0] + ":" + s._2().split(" ")[1]
                                            +":"+ s._2().split(" ")[2]+":"+s._2().split(" ")[3]
                                            +":"+ s._2().split(" ")[4]+":"+s._2().split(" ")[5]
                                            +":"+ s._2().split(" ")[6]);
                    }
                });

        allInfo.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            public Void call(JavaPairRDD<String, String> rdd) {
                Jedis jedis = new Jedis("localhost");
                for (Tuple2<String, String> t: rdd.collect()) {
                    Map<String, String> coordinateAsHash = new HashMap<String, String>();
                    coordinateAsHash.put("latitude", t._2().split(":")[0]);
                    coordinateAsHash.put("longitude", t._2().split(":")[1]);
                    coordinateAsHash.put("altitude", t._2().split(":")[2]);
                    coordinateAsHash.put("speed", t._2().split(":")[3]);
                    coordinateAsHash.put("aircraftModel", t._2().split(":")[4]);
                    coordinateAsHash.put("origin", t._2().split(":")[5]);
                    coordinateAsHash.put("destination", t._2().split(":")[6]);
                    jedis.hmset(t._1(), coordinateAsHash);
                }
                return null;
            }
        });

        //allInfo.print();
        //latitude.print();
        planeCount.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}