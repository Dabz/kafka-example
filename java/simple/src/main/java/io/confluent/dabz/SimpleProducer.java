package io.confluent.dabz;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleProducer implements Runnable {

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "blah");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        properties.load(new FileInputStream("/home/gaspar_d/.ccloudkafka.properties"));

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        kafkaProducer.initTransactions();

        kafkaProducer.beginTransaction();
        kafkaProducer.beginTransaction();


        ExecutorService pool = Executors.newFixedThreadPool(1);
        for (int i = 0; i < 1; i++) {
            pool.execute(() -> {
                for (int j = 0; j < 50; j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("ts", 0, 0L, String.valueOf(new Random().nextInt()), "test");
                    kafkaProducer.beginTransaction();
                    kafkaProducer.send(record, (metadata, error) -> {
                        if (error != null) {
                            System.err.print(error.getCause());
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
        try {
            main(null);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
