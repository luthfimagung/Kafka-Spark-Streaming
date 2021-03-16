package com.example;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkStreamingKafka {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("ExampleStream").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(500));

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Map<String, Object> kafkaParams = new HashMap<>();
        // kafkaParams.put("bootstrap.servers", "l=localhost:9092");
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkstreamingkafka");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test1");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        stream.mapToPair(record -> new Tuple2<>(record.key().toString(), record.value().toString()));

        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> v1) throws Exception {
                // System.out.println(v1.topic()+" =====> "+v1.value());
                // System.out.println(v1.value());
                return v1.value();
            }
        });

        // lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
        //     @Override
        //     public void call(JavaRDD<String> t) throws Exception {
        //         System.out.println("- " + t);
        //     }
        // });

        lines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
