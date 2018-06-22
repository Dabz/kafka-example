package io.confluent.dabz;

import io.confluent.dabz.model.ShakespeareKey;
import io.confluent.dabz.model.ShakespeareValue;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Int;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class AvroConsumer {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaConsumer<ShakespeareKey, ShakespeareValue> consumer = new KafkaConsumer<ShakespeareKey, ShakespeareValue>(properties);
        consumer.subscribe(Arrays.asList("shake"));

        while (true) {
            ConsumerRecords<ShakespeareKey, ShakespeareValue> consumerRecords = consumer.poll(60);
            for (ConsumerRecord<ShakespeareKey, ShakespeareValue> record: consumerRecords) {
                System.out.println(String.format("Line %d is %s", record.value().getLineNumber(), record.value().getLine()));
            }
        }
    }
}
