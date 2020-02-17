package io.confluent.dabz;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.tools.ConsoleConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class MainTest {

    private EmbeddedZookeeper zookeeper;
    private EmbeddedKafkaCluster kafka;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws IOException, InterruptedException {
        zookeeper = new EmbeddedZookeeper();
        zookeeper.startup();

        kafka = new EmbeddedKafkaCluster("localhost:2181");
        kafka.startup();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        kafka.shutdown();
        zookeeper.shutdown();
    }

    @org.junit.jupiter.api.Test
    void main() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.send(new ProducerRecord<>("test", "test", "test")).get();
        kafkaProducer.close();

        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "blahblahblah");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test"));
        for (int i = 0; i < 100; i++) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (! records.isEmpty()) {
                ConsumerRecord<String, String> next = records.iterator().next();
                if (next.value().equals("test")) {
                    return;
                }
            }

            Thread.sleep(100);
        }

        assert true;
    }
}