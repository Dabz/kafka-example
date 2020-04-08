package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class MainTest {

    private EmbeddedZookeeper zookeeper;
    private EmbeddedKafkaCluster kafka;
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaConsumer<String, String> kafkaConsumer;
    private AdminClient kafkaAdmin;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws IOException, InterruptedException {
        zookeeper = new EmbeddedZookeeper(2180);
        zookeeper.startup();

        Properties properties = new Properties();
        properties.setProperty("offsets.topic.replication.factor", "1");
        properties.setProperty("default.topic.replication.factor", "1");
        properties.setProperty("transactions.topic.replication.factor", "1");
        properties.setProperty("group.initial.rebalance.delay.ms", "0");
        kafka = new EmbeddedKafkaCluster("localhost:2180",properties, Arrays.asList(9096));
        kafka.startup();

        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9096");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaProducer = new KafkaProducer<>(properties);
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaAdmin = KafkaAdminClient.create(properties);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        kafka.shutdown();
        zookeeper.shutdown();
    }

    @Test
    void testYourApplicationHere() throws Exception {
    }

    @org.junit.jupiter.api.Test
    void ensureWorking() throws ExecutionException, InterruptedException {
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("stream", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("table", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("streams-linesplit-table-changelog-changelog", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("result", 1, (short) 1)));

        Properties props = getProperties();
        final StreamsBuilder builder = new StreamsBuilder();
        SimpleKStream.withoutRepartition(builder);
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        kafkaProducer.send(new ProducerRecord<>("stream", "1", "2|hello")).get();
        kafkaProducer.send(new ProducerRecord<>("table", "1", "1|hello")).get();
        kafkaProducer.send(new ProducerRecord<>("stream", "justtounblockprocessing", "3|hello")).get();
        kafkaProducer.send(new ProducerRecord<>("table", "justtounblockprocessing", "3|hello")).get();

        kafkaConsumer.subscribe(Arrays.asList("result"));
        for (int i = 0; i < 300; i++) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            if (! records.isEmpty()) {
                return;
            }
        }
        assert false;
    }

    @org.junit.jupiter.api.Test
    void withRepartition() throws ExecutionException, InterruptedException {
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("stream", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("table", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("streams-linesplit-table-changelog-changelog", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("result", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("repartition", 1, (short) 1)));

        Properties props = getProperties();
        final StreamsBuilder builder = new StreamsBuilder();
        SimpleKStream.withRepartition(builder);
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        kafkaProducer.send(new ProducerRecord<>("stream", "1", "2|hello")).get();
        kafkaProducer.send(new ProducerRecord<>("table", "1", "1|hello")).get();
        kafkaProducer.send(new ProducerRecord<>("stream", "justtounblockprocessing", "3|hello")).get();
        kafkaProducer.send(new ProducerRecord<>("table", "justtounblockprocessing", "3|hello")).get();

        kafkaConsumer.subscribe(Arrays.asList("result"));
        for (int i = 0; i < 300; i++) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            if (! records.isEmpty()) {
                return;
            }
        }
        assert false;
    }


    @org.junit.jupiter.api.Test
    void ktableVersioning() throws ExecutionException, InterruptedException {
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("stream", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("table", 1, (short) 1)));
        kafkaAdmin.createTopics(Collections.singleton(new NewTopic("result", 1, (short) 1)));

        Properties props = getProperties();
        final StreamsBuilder builder = new StreamsBuilder();
        SimpleKStream.withoutRepartition(builder);
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();

        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>("table", "1", String.format("%d|table", i))).get();
        }

        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>("stream", "1", String.format("%d|stream", i))).get();
        }

        kafkaConsumer.subscribe(Arrays.asList("result"));
        for (int i = 0; i < 300; i++) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }


    private Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9096");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, Long.MAX_VALUE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);
        return props;
    }
}