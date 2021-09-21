package io.confluent.dabz;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "check_consumer_group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "check_consumer_group");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        var lastCommitPerGroup = new HashMap<String, Long>();
        var deletedGroup = new HashMap<String, Long>();
        consumer.subscribe(Collections.singleton("__consumer_offsets"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                consumer.seekToBeginning(consumer.assignment());
            }
        });

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(5000));
            if (records.isEmpty()) {
                break;
            }
            for (var record : records) {
                String groupId = record.key();
                Boolean isTombstone = record.value() == null;

                if (isTombstone) {
                    deletedGroup.put(groupId, record.timestamp());
                    lastCommitPerGroup.remove(groupId);
                } else {
                    lastCommitPerGroup.put(groupId, record.timestamp());
                    deletedGroup.remove(groupId);
                }
            }
        }
        consumer.close();

        var currentDate = Instant.now().toEpochMilli();
        Set<Map.Entry<String, Long>> groupThatWillBeDeleted = lastCommitPerGroup.entrySet().stream()
                .filter((entry) -> currentDate > (entry.getValue() + (7 * 24 * 60 * 60 * 1000)))
                .collect(Collectors.toSet());

        if (! deletedGroup.isEmpty()) {
            System.out.println("Deleted groups & topics:");
            deletedGroup.forEach((k, v) -> {
                System.out.println(String.format("\t%s: %s", k, new Date(v).toString()));
            });
        }

        if (! groupThatWillBeDeleted.isEmpty()) {
            System.out.println("Group with last committed offset older than 7 days:");
            lastCommitPerGroup.forEach((k, v) -> {
                System.out.println(String.format("\t%s: %s", k, new Date(v).toString()));
            });
        }
    }
}
