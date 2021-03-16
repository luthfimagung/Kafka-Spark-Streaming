package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Producers {
    final static String TOPIC = "test1";
    public static void main( String[] args ) throws InterruptedException
    {
        Logger.getLogger("org").setLevel(Level.ERROR);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // props.put("acks", "all");
        // props.put("retries", 0);
        // props.put("batch.size", 16384);
        // props.put("linger.ms", 1);
        // props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 1; i <= 50; i++) {
            Thread.sleep(500);
            producer.send(new ProducerRecord<String, String>("test1", "key-" + i, "Message from Producer - " + i));
        }
        producer.close();
    }
}
