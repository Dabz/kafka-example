package io.confluent.dabz;

import io.confluent.dabz.model.ShakespeareKey;
import io.confluent.dabz.model.ShakespeareMySecondValue;
import io.confluent.dabz.model.ShakespeareValue;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import scala.Int;

import java.io.*;
import java.util.*;

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
        properties.setProperty(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, ShakeSubjectValueStrategy.class.getName());
        properties.setProperty(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, ShakeSubjectValueStrategy.class.getName());

        KafkaConsumer<ShakespeareKey, SpecificRecord> consumer = new KafkaConsumer<ShakespeareKey, SpecificRecord>(properties);
        consumer.subscribe(Arrays.asList("bouga2"));

        while (true) {
            ConsumerRecords<ShakespeareKey, SpecificRecord> consumerRecords = consumer.poll(60);
            for (ConsumerRecord<ShakespeareKey, SpecificRecord> record: consumerRecords) {
                if (record.value() instanceof ShakespeareValue) {
                    ShakespeareValue value = (ShakespeareValue) record.value();
                    System.out.println(value.getLine());
                }
                if (record.value() instanceof ShakespeareMySecondValue) {
                    ShakespeareMySecondValue value = (ShakespeareMySecondValue) record.value();
                    System.out.println(value.getBlah());
                }
                if (record.value() instanceof Exception) {
                    System.out.println("Unknown message");
                }

                System.out.println(record.value().getClass().getName());
            }
        }
    }
}
