package io.confluent.dabz;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleProducer implements Runnable {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "blah");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        kafkaProducer.initTransactions();

        ExecutorService pool = Executors.newFixedThreadPool(1);
        for (int i = 0; i < 1; i++) {
            pool.execute(() -> {
                while (true) {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", String.valueOf(new Random().nextInt()), "test");
                    kafkaProducer.beginTransaction();
                    kafkaProducer.send(record, (metadata, error) -> {
                        if (error != null) {
                            System.err.print(error.getCause().toString());
                        } else {
                            System.out.println("ok");
                        }
                    });
                    kafkaProducer.commitTransaction();

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    @Override
    public void run() {
        main(null);
    }
}
