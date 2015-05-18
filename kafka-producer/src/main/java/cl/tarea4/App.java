package cl.tarea4;

import com.google.common.collect.ImmutableMap;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;
import java.net.*;
import java.io.*;

public class App {
    public static void main(String[] args) throws IOException {
        /*
        Thread consumerThread = new Thread(new PrintingConsumer());
        consumerThread.setName("Kafka Consumer Thread");
        consumerThread.start();
        */

        Timer timer = new Timer();
        String url = "http://arn.data.fr24.com/zones/fcgi/full_all.js";
        URLFetchTask fetchTask = new URLFetchTask(url);
        timer.schedule(fetchTask, 0,30);
    }

    /*
    static class PrintingConsumer implements Runnable{
        @Override
        public void run(){

            Properties properties = new Properties();
            properties.put("zookeeper.connect", "localhost:2181");
            properties.put("group.id", "chatconsumer");
            ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

            Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(ImmutableMap.of("testTopic", 1));

            KafkaStream<byte[], byte[]> stream = streamMap.get("testTopic").get(0);

            for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
                System.out.printf("%s => %s\n", new String(messageAndMetadata.key()), new String(messageAndMetadata.message()));
            }

        }
    }
    */

    public static class URLFetchTask extends TimerTask {

        private URL url;

        public URLFetchTask(String url) throws MalformedURLException {
            this.url = new URL(url);
        }

        public void run() {

            Properties properties = new Properties();
            properties.put("metadata.broker.list", "localhost:9092");

            Producer<byte[], byte[]> producer = new Producer<>(new ProducerConfig(properties));

            System.out.println("Fetching from " + url);
            InputStreamReader inputStream = null;
            try {
                inputStream = new InputStreamReader(url.openStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
            BufferedReader in = new BufferedReader(inputStream);
            String flightInfo;

            FlightInfoJsonParser parser = new FlightInfoJsonParser();
            FlightInfoValidator validator = new FlightInfoValidator();

            int count = 0;
            try {
                while ((flightInfo = in.readLine()) != null) {

                    if (count == 0) {
                        count++;
                        continue;
                    }

                    if (!validator.isValidFlightInfo(flightInfo)) {
                        continue;
                    }

                    parser.setFlightInfo(flightInfo);

                    String key = parser.getKey();
                    String latitude = parser.getLatitude();
                    String longitude = parser.getLongitude();
                    String altitude = parser.getAltitude();
                    String speed = parser.getSpeed();
                    String aircraftModel = parser.getAircraftModel();
                    String origin = parser.getOrigin();
                    String destiny = parser.getDestiny();

                    String message = latitude+" "+longitude+" "+altitude+" "+speed+" "+ aircraftModel+" "+origin+" "+destiny;
                    producer.send(new KeyedMessage<>("testTopic", key.getBytes(),message.getBytes() ) );
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("SE ACABO");
            producer.close();
        }
    }
}
