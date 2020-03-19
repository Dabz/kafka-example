package io.confluent.dabz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class SimpleProducer implements Runnable {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(1024 * 1024 * 32));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(1000));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(100000));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<Integer, Integer> kafkaProducer = new KafkaProducer<>(properties);

        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(1024 * 1024 * 32));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(1000));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(100000));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<byte[], byte[]> kafkaIntegerProducer = new KafkaProducer<>(properties);

        Integer i = 1;
        while (true) {
            i++;
            ProducerRecord<Integer, Integer> record = new ProducerRecord<>("poisonpill", 1, 1);
            kafkaProducer.send(record, (metadata, error) -> {
                if (error != null) {
                    System.err.print(error.getCause().toString());;
                }
            });

            if (i % 10 == 0) {
                byte[] b = new byte[20];
                new Random().nextBytes(b);
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("poisonpill", b, b);
                kafkaIntegerProducer.send(producerRecord, ((metadata, exception) -> {
                    if (exception != null) {
                        System.err.print(exception.getCause().toString());;
                    } else {
                        System.out.println("ok");
                    }

                }));
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        main(null);
    }
}
