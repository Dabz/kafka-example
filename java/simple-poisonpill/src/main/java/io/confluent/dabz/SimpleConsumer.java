package io.confluent.dabz;

import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class SimpleConsumer implements Runnable {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("poisonpill"));

        while (true) {
            try {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(60);
                for (ConsumerRecord record : consumerRecords) {
                    System.out.println(String.format("Received message from partition %d with offset %d", record.partition(), record.offset()));
                }
            } catch (SerializationException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        main(null);
    }
}
