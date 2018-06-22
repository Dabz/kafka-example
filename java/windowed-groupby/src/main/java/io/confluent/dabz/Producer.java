package io.confluent.dabz;

import io.confluent.dabz.model.VisitEventKey;
import io.confluent.dabz.model.VisitEventValue;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producer implements Runnable {

    @Override
    public void run() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MonitoringProducerInterceptor.class.getName());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(producerProperties);
        SpecificAvroSerde<SpecificRecord> avroSerde = new SpecificAvroSerde<>();

        Integer currentDay = (2010 - 1970) * 360;
        Integer messageCounter = 0;
        final int MESSAGE_PER_DAY = 100000;
        final String[] pages = {"/index", "/products/0", "/products/1", "/products/2", "/checkout"};

        try {
            while (true) {
                VisitEventKey key = new VisitEventKey();
                key.setDay(currentDay);
                key.setPage(pages[new Random().nextInt(pages.length)]);

                VisitEventValue value = new VisitEventValue();
                value.setUser(String.valueOf(new Random().nextInt(1000)));
                value.setTime(((3600 * 1000) / MESSAGE_PER_DAY) * messageCounter);
                value.setTimeSpent((long) Math.abs(new Random().nextInt(1000)));

                producer.send(new ProducerRecord<>("visits", key, value));

                messageCounter += 1;
                if ((messageCounter % MESSAGE_PER_DAY) == 0) {
                    currentDay += 1;
                }

                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();

        }
    }
}
