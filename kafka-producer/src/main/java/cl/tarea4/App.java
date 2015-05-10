package cl.tarea4;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.net.*;
import java.io.*;

public class App 
{
    public static void main( String[] args ) throws IOException {
        // Producer initialization
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");

        Producer<byte[], byte[]> producer = new Producer<>(new ProducerConfig(properties));

        // Fetching the flightRadar URL
        URL flightRadarInfo = new URL("http://arn.data.fr24.com/zones/fcgi/full_all.js");
        BufferedReader in = new BufferedReader( new InputStreamReader(flightRadarInfo.openStream()) );
        String flightInfo;
        while ( (flightInfo = in.readLine()) != null)
        {
            producer.send(new KeyedMessage<>("testTopic", "key".getBytes(), flightInfo.getBytes()) );
        }

        in.close();
        producer.close();
    }
}
