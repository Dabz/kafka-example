package io.confluent.dabz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class FirstImplem {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // First possible implementation based on a PriorityQueue.
        // The easiest one, but not perfect as we are not waiting for
        // all partitions

        // 3rd Problem: Need to be smarter with offsetcommit
        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("A"));
        var priorityQueue = new PriorityQueue<ConsumerRecord<String, String>>(Comparator.comparingLong(ConsumerRecord::timestamp));
        AtomicBoolean isRunning = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isRunning.set(false)));

        while (isRunning.get()) {
            // Note: poll() might not return all data from Kafka, so some record could be
            // processed out of order as the consumer didn't retrieve them yet
            var records = consumer.poll(Duration.ofMillis(100));
            records.forEach((record) -> priorityQueue.add(record));
            priorityQueue.stream().forEach((record) -> {
                // Do Business here
            });
        }

    }
}
