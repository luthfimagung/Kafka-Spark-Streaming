package com.example;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Consumers {
    
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        Properties props = new Properties();
        // props.put("bootstrap.servers", "l=localhost:9092,localhost2:9092");
        props.put("bootstrap.servers", "localhost:9092");

        props.put("group.id", "group." + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("topic = %s, value = %s, timestamp = %s", record.topic(), record.value(),
                        record.timestamp() + "\n");
        }
    }
}
