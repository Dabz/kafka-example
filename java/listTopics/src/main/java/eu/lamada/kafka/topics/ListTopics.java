package eu.lamada.kafka.topics;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ListTopics {
    final static Logger logger = LoggerFactory.getLogger(ListTopics.class);
    final static Long MINUTES_TO_CONSIDER_STALE = 10L;
    private static List<String> infrequentlyUsedTopics;
    private static List<String> emptyTopics;
    private static KafkaConsumer<Bytes, Bytes> kafkaConsumer;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length < 1) {
            logger.error("Missing Apache Kafka configuration file");
            return;
        }

        Properties properties = new Properties();
        properties.load(new FileInputStream(args[0]));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        AdminClient adminClient = KafkaAdminClient.create(properties);
        kafkaConsumer = new KafkaConsumer<>(properties);

        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        Set<String> applicationTopicNames = topics.stream()
                .filter((topic) -> !topic.isInternal())
                .filter((topic) -> !topic.name().startsWith("_"))
                .map((topic) -> topic.name())
                .collect(Collectors.toSet());
        Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(applicationTopicNames).all().get();

        infrequentlyUsedTopics = new ArrayList<>();
        emptyTopics = new ArrayList<>();

        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
            checkTopic(entry);
        }

        logger.info("Empty topics: " + String.join(",", emptyTopics));
        logger.info("Topic with no recent messages: " + String.join(",", infrequentlyUsedTopics));

        kafkaConsumer.close();
        adminClient.close();
    }

    private static void checkTopic(Map.Entry<String, TopicDescription> entry) {
        List<TopicPartitionInfo> partitions = entry.getValue().partitions();
        Date now = new Date();
        AtomicLong biggestTimestamp = new AtomicLong(0L);

        kafkaConsumer.assign(partitions.stream().map((topic) -> new TopicPartition(entry.getKey(), topic.partition())).collect(Collectors.toList()));
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions.stream().map((topic) -> new TopicPartition(entry.getKey(), topic.partition())).collect(Collectors.toList()));
        AtomicBoolean topicIsEmpty = new AtomicBoolean(true);

        endOffsets.forEach(((topicPartition, aLong) -> {
            if (aLong > 0) {
                topicIsEmpty.set(false);
            }
            endOffsets.put(topicPartition, Math.max(0, aLong - 5));
        }));

        if (topicIsEmpty.get()) {
            emptyTopics.add(entry.getKey());
            return;
        }

        for (Map.Entry<TopicPartition, Long> partitionLongEntry : endOffsets.entrySet()) {
            kafkaConsumer.seek(partitionLongEntry.getKey(), partitionLongEntry.getValue());
        }

        ConsumerRecords<Bytes, Bytes> records = kafkaConsumer.poll(Duration.ofSeconds(10));

        if (records.isEmpty()) {
            emptyTopics.add(entry.getKey());
            return;
        }

        records.forEach((record) -> {
            biggestTimestamp.set(Math.max(biggestTimestamp.get(), record.timestamp()));
        });

        if ((now.getTime() - biggestTimestamp.get()) > (MINUTES_TO_CONSIDER_STALE * 60 * 1000)) {
            infrequentlyUsedTopics.add(entry.getKey());
        }
    }
}
