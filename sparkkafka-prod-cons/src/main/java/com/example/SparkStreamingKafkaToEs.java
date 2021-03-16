package com.example;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import scala.Tuple2;

public class SparkStreamingKafkaToEs {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("ExampleStream").setMaster("local[2]")
            .set("es.index.auto.create", "true")
            .set("es.nodes.wan.only", "true")
            .set("es.nodes", "192.168.20.245:9200");
            // .set("es.nodes", "localhost:9200");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(1000));

        // Logger rootLogger = Logger.getRootLogger();
        // rootLogger.setLevel(Level.OFF);

        Map<String, Object> kafkaParams = new HashMap<>();
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
                String json = "";
                ObjectMapper mapper = new ObjectMapper();
                try {
                    Map<String, Object> result = new HashMap<String, Object>();
                    ZonedDateTime zone = ZonedDateTime.now(ZoneId.of("Asia/Jakarta"));
                    DateTimeFormatter dateformat = DateTimeFormatter.ISO_INSTANT;
                    String input_date = zone.format(dateformat);
                    result.put("topic", v1.topic());
                    result.put("message", v1.value());
                    result.put("input_date", input_date);
                    json = mapper.writeValueAsString(result);
                } catch (Exception e) {
                    e.getMessage();
                }
                return json;
            }
        });

        try {
            JavaEsSparkStreaming.saveJsonToEs(lines, "sparkkafkatest/docs");
        } catch (Exception e) {
            e.getMessage();
        }

        lines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
