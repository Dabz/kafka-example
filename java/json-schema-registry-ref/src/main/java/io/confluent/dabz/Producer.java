package io.confluent.dabz;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.setProperty(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.setProperty(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, "true");
        properties.setProperty(KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, "true");
        properties.setProperty(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, "false");

        KafkaProducer<String, School> producer = new KafkaProducer<>(properties);
        School school = new School();
        school.setId(4);
        school.setName("Hello");
        Student student = new Student();
        student.setName("Mathieu");
        student.setSchool(school);

        producer.send(new ProducerRecord("h", "mathieu", student)).get();
        producer.close();
    }
}
