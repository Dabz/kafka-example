package io.confluent.dabz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SecondImplem {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        AtomicBoolean isRunning = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isRunning.set(false)));

        // Second possible implementation based on a PriorityQueue.
        // Here, we keep track of a Watermark and process only messages
        // that is bellow the Watermark
        // It should ensure proper ordering, but it also implies that you
        // need to receiver messages in all TopicPartition

        // Problem: slow TopicPartition block all messages as the WM would not be updated

        // Another problem with this current implementation, I am not initializing the timestampPerTopicPartition
        // properly, due to that, first few messages could not be properly ordered as
        // the in-memory Map might not have an entry for each TopicPartition

        // 3rd Problem: Need to be smarter with offsetcommit

        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("A"));
        var priorityQueuePerTopicPartition = new HashMap<TopicPartition, PriorityQueue<ConsumerRecord<String, String>>>();
        var timestampPerTopicPartition = new HashMap<TopicPartition, Long>();
        var highWaterMark = 0L;


        while (isRunning.get()) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (var record : records) {
                var topicPartition = new TopicPartition(record.topic(), record.partition());
                // Step 1. Update the timestamp for this Topic Partition
                timestampPerTopicPartition.put(topicPartition, record.timestamp());
                // Step 2. Add record to the TopicPartition PriorityQueue
                addRecordToPriorityQueue(priorityQueuePerTopicPartition, record, topicPartition);
                // Step 3. Update the Watermark to the min of all TopicPartition
                highWaterMark = updateWaterMark(timestampPerTopicPartition, record, topicPartition);
                // Step 4. Process all messages that has a timestamp lesser than the Watermark
                processedUpToWaterMark(priorityQueuePerTopicPartition, highWaterMark);
            }
        }
    }

    private static void addRecordToPriorityQueue(HashMap<TopicPartition, PriorityQueue<ConsumerRecord<String, String>>> priorityQueuePerTopicPartition, ConsumerRecord<String, String> record, TopicPartition topicPartition) {
        var priorityQueue = priorityQueuePerTopicPartition.getOrDefault(topicPartition, new PriorityQueue<>(Comparator.comparingLong(ConsumerRecord::timestamp)));
        priorityQueue.add(record);
        priorityQueuePerTopicPartition.put(topicPartition, priorityQueue);
    }

    private static void processedUpToWaterMark(HashMap<TopicPartition, PriorityQueue<ConsumerRecord<String, String>>> priorityQueues, Long waterMark) {
        for (Map.Entry<TopicPartition, PriorityQueue<ConsumerRecord<String, String>>> priorityQueueTopic : priorityQueues.entrySet()) {
            var priorityQueue = priorityQueueTopic.getValue();
            while (! priorityQueue.isEmpty()) {
                var peek = priorityQueue.peek();
                if (peek.timestamp() > waterMark) {
                    break;
                }
                var message = priorityQueue.poll();
                // Do business here
            }
        }

    }

    private static long updateWaterMark(HashMap<TopicPartition, Long> timestampPerTopicPartition, ConsumerRecord<String, String> record, TopicPartition topicPartition) {
        long highWaterMark;
        highWaterMark = record.timestamp();
        for (var topicPartitionLongEntry : timestampPerTopicPartition.entrySet()) {
            if (topicPartitionLongEntry.getKey().equals(topicPartition)) {
                if (highWaterMark > topicPartitionLongEntry.getValue()) {
                    highWaterMark = topicPartitionLongEntry.getValue();
                }
            }
        }
        return highWaterMark;
    }
}
