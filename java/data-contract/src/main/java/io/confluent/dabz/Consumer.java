package io.confluent.dabz;

import com.sun.tools.javac.Main;
import io.confluent.dabz.model.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(new Consumer().getClass().getResourceAsStream("/consumer.properties"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        try (KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("customer"));
            while (true) {
                try {
                    ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofSeconds(1));
                    for (var record : records) {
                        System.out.println(record.value().toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }
}
