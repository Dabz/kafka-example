package io.confluent.dabz;

import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;


public class SimpleConsumer implements Runnable {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "daminmou-app");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("ts"));
        consumer.seekToEnd(consumer.assignment());

        try {
            while (true) {
                try {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : consumerRecords) {
                        System.out.println(String.format("Received message from partition %d with offset %d", record.partition(), record.offset()));
                    } } catch (Exception e) {
                    // sendToDlq
                    // log the error
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void run() {
        main(null);
    }
}
