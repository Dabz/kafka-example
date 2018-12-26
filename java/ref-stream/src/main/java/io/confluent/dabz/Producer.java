package io.confluent.dabz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import java.util.Properties;

public class Producer implements Runnable {

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        KafkaProducer producer = new KafkaProducer(properties, new StringSerializer(), new StringSerializer());

        Integer sequence = 0;
        try {
        while (true) {
            producer.send(new ProducerRecord<String, String>("ref-stream", "test", "hello"));
            Thread.sleep(1000);
            sequence = sequence + 1;
        }
        } catch (InterruptedException e) {
        }
    }
}
