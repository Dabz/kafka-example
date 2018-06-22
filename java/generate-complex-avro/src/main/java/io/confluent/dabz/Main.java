package io.confluent.dabz;

import io.confluent.dabz.model.Key;
import io.confluent.dabz.model.Record;
import io.confluent.dabz.model.Value;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Properties producerProperty = new Properties();

        producerProperty.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperty.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperty.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperty.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<Key, Value> kafkaProducer = new KafkaProducer<Key, Value>(producerProperty);

        for (int i = 0; i < 100; i++) {
            Key key = new Key();
            key.setTitle(String.valueOf(i));

            Value value = new Value();
            value.setTitle("a title?");

            Record record1 = new Record();
            record1.setLine("line 1");

            Record record2 = new Record();
            record2.setLine("line 2");

            value.setArray(Arrays.asList(record1, record2));

            kafkaProducer.send(new ProducerRecord<Key, Value>("avro-array", key, value));
        }
    }
}
