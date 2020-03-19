package io.confluent.dabz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import java.util.Properties;
import java.util.UUID;

public class Producer implements Runnable {

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer producer = new KafkaProducer(properties, new StringSerializer(), new StringSerializer());

        String uuid = UUID.randomUUID().toString();
        Integer sequence = 0;
        try {
        while (true) {
            producer.send(new ProducerRecord<String, String>("table-1", uuid + sequence.toString(), "hello"));
            producer.send(new ProducerRecord<String, String>("table-2", uuid + sequence.toString(), "world"));
            Thread.sleep(50);
            sequence = sequence + 1;
        }
        } catch (InterruptedException e) {
        }
    }
}
