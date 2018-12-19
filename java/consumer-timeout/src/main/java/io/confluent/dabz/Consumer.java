package io.confluent.dabz;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Consumer implements Runnable {
    public void run() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-timeout");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("consumer-timeout"));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                ConsumerRecord firstRecords = null;
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(record.value());
                    firstRecords = record;
                    break;
                }

                Thread.sleep(15000);

                HashMap<TopicPartition, OffsetAndMetadata> hashMap = new HashMap<>();
                for (TopicPartition topicPartition : kafkaConsumer.assignment()) {
                    hashMap.put(topicPartition, new OffsetAndMetadata(firstRecords.offset()));
                }

                kafkaConsumer.commitSync(hashMap);
            }
        } catch (InterruptedException e) {

        }
    }
}
