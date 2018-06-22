package io.confluent.dabz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(1024 * 1024 * 32));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(1000));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(100000));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        for (int i = 0; i < 16; i++) {
            pool.execute(() -> {
                while (true) {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", String.valueOf(new Random().nextInt()), "test");
                    kafkaProducer.send(record);
                }
            });
        }

        try {
            pool.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            pool.shutdown();
        }
    }
}
