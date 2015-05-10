package cl.tarea4;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class App
{

    public static void main(String[] args) {

        String brokers = args[0];
        String topics = args[1];

        // Create context with 2 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("App");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

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


        // Get the lines, count the lines in this case
        messages.foreach(new Function<JavaPairRDD<String, String>, Void>(){
            @Override
            public Void call(JavaPairRDD<String, String> javaPairRDD) throws Exception {
                System.out.println("RDD Count: " + javaPairRDD.count());
                return null;
            }
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}