package io.confluent.dabz;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class MainTest {

    private EmbeddedZookeeper zookeeper;
    private EmbeddedKafkaCluster kafka;
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaConsumer<Object, Object> kafkaConsumer;
    private AdminClient adminClient;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        zookeeper = new EmbeddedZookeeper(2181);
        zookeeper.startup();

        Properties properties = new Properties();
        properties.setProperty("offsets.topic.replication.factor", "1");
        properties.setProperty("default.topic.replication.factor", "1");
        properties.setProperty("transactions.topic.replication.factor", "1");
        kafka = new EmbeddedKafkaCluster("localhost:2181",properties, Arrays.asList(9092));
        kafka.startup();

        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(properties);

        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "blahblahblah");

        kafkaConsumer = new KafkaConsumer<>(properties);

        adminClient = KafkaAdminClient.create(properties);
    }

    @AfterEach
    void tearDown() {
        kafkaConsumer.close();
        kafkaProducer.close();
        kafka.shutdown();
        zookeeper.shutdown();
    }

    /**
     * Simple upgrade of the logic of a map.
     * This has no impact on the application and can be done
     * a rolling way
     */
    @Test
    void simpleUpgrade() throws ExecutionException, InterruptedException {
        kafkaProducer.send(new ProducerRecord("simpleUpgrade-input", "x", "x")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream("simpleUpgrade-input")
                .map((k, v) -> new KeyValue<>(k, v))
                .to("simpleUpgrade-output");

        Properties properties = TestUtils.getStreamProperties("simple-upgrade");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, "simpleUpgrade-output", 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("x", record.key());

        kafkaStreams.close();

        kafkaProducer.send(new ProducerRecord("simpleUpgrade-input", "x", "x")).get();

        builder = new StreamsBuilder();
        builder
                .stream("simpleUpgrade-input")
                .map((k, v) -> new KeyValue<>("toto", "toto"))
                .to("simpleUpgrade-output");

        properties = TestUtils.getStreamProperties("simple-upgrade");
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        record = TestUtils.pollNextRecord(kafkaConsumer, "simpleUpgrade-output", 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("toto", record.key());

        kafkaStreams.close();
    }

    /**
     * Simple rolling upgrade of a stateless application
     * Works perfectly, but there might be a transition period
     */
    @Test
    void rollingUpgrade() throws ExecutionException, InterruptedException {
        String inputTopic = "simpleUpgrade-input";
        String outputTopic = "simpleUpgrade-output";

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "x")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .map((k, v) -> new KeyValue<>(k, v))
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties("simple-upgrade");
        KafkaStreams kafkaStreams1 = new KafkaStreams(builder.build(), properties);
        KafkaStreams kafkaStreams2 = new KafkaStreams(builder.build(), properties);
        kafkaStreams1.start();
        kafkaStreams2.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("x", record.key());

        builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .map((k, v) -> new KeyValue<>("toto", "toto"))
                .to(outputTopic);


        kafkaStreams1.close();
        kafkaStreams1 = new KafkaStreams(builder.build(), properties);
        kafkaStreams1.start();
        kafkaStreams2.close();
        kafkaStreams2 = new KafkaStreams(builder.build(), properties);
        kafkaStreams2.start();

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "x")).get();

        builder = new StreamsBuilder();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("toto", record.key());

        kafkaStreams1.close();
        kafkaStreams2.close();
    }

    /**
     * Rolling upgrade while adding a new subtopology
     */
    @Test
    void rollingUpgradeWithSubTopology() throws ExecutionException, InterruptedException {
        String inputTopic = "simpleUpgrade-input";
        String outputTopic = "simpleUpgrade-output";
        String throughTopic = "simpleUpgrade-through";

        adminClient.createTopics(Collections.singleton(new NewTopic(throughTopic, 1, (short) 1)));
        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "x")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties("simple-upgrade");
        KafkaStreams kafkaStreams1 = new KafkaStreams(builder.build(), properties);
        KafkaStreams kafkaStreams2 = new KafkaStreams(builder.build(), properties);
        kafkaStreams1.start();
        kafkaStreams2.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("x", record.key());

        builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .map((k, v) -> new KeyValue<>("toto", "toto"))
                .through(throughTopic)
                .map((k, v) -> new KeyValue<>("toto", "toto"))
                .to(outputTopic);


        kafkaStreams1.close();
        kafkaStreams1 = new KafkaStreams(builder.build(), properties);
        kafkaStreams1.start();
        kafkaStreams2.close();
        kafkaStreams2 = new KafkaStreams(builder.build(), properties);
        kafkaStreams2.start();

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "x")).get();

        builder = new StreamsBuilder();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("toto", record.key());

        kafkaStreams1.close();
        kafkaStreams2.close();
    }


    /**
     * Simple rolling upgrade with a topology chang
     */
    @Test
    void rollingUpgradeWithTopologyChange() throws ExecutionException, InterruptedException {
        String inputTopic = "simpleUpgrade-input";
        String outputTopic = "simpleUpgrade-output";

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "x")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties("simple-upgrade");
        KafkaStreams kafkaStreams1 = new KafkaStreams(builder.build(), properties);
        KafkaStreams kafkaStreams2 = new KafkaStreams(builder.build(), properties);
        kafkaStreams1.start();
        kafkaStreams2.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("x", record.key());

        builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .map((k, v) -> new KeyValue<>("toto", "toto"))
                .to(outputTopic);


        kafkaStreams1.close();
        kafkaStreams1 = new KafkaStreams(builder.build(), properties);
        kafkaStreams1.start();
        kafkaStreams2.close();
        kafkaStreams2 = new KafkaStreams(builder.build(), properties);
        kafkaStreams2.start();

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "x")).get();

        builder = new StreamsBuilder();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("toto", record.key());

        kafkaStreams1.close();
        kafkaStreams2.close();
    }


    /**
     * Demonstrate a slight change of topology by adding a map.
     * Although it is possible, we can not do a rolling deployment
     * while changing the topology
     */
    @Test
    void simpleChangeOfTopology() throws ExecutionException, InterruptedException {
        String intputTopic = "simpleChangeOfTopology-input";
        String outputTopic = "simpleChangeOfTopology-output";
        kafkaProducer.send(new ProducerRecord(intputTopic, "x", "x")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(intputTopic)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties("simple-change-topology");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("x", record.key());

        kafkaStreams.close();

        kafkaProducer.send(new ProducerRecord(intputTopic, "x", "x")).get();

        builder = new StreamsBuilder();
        builder
                .stream(intputTopic)
                .map((k, v) -> new KeyValue<>("toto", "toto"))
                .to(outputTopic);

        properties = TestUtils.getStreamProperties("simple-change-topology");
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("toto", record.key());

        kafkaStreams.close();
    }


    /**
     * Simple state store change where we "refresh", aka resend, all data to the
     * input topic once we change the topology
     */
    @Test
    void simpleStateStoreChange() throws ExecutionException, InterruptedException {
        String inputTopic = "simpleStateChange-input";
        String outputTopic = "simpleStateChange-output";
        String tableTopic = "simpleStateChange-table";
        String appId = "simpleStateChange";

        kafkaProducer.send(new ProducerRecord(tableTopic, "x", "table")).get();
        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "input")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .join(builder.table(tableTopic), (l, r) -> l + " " + r)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties(appId);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input table", record.value());

        kafkaStreams.close();

        kafkaProducer.send(new ProducerRecord(tableTopic, "x", "table")).get();
        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "input")).get();

        builder = new StreamsBuilder();
        KTable<String, String> table = builder
                .table(tableTopic, Materialized.with(Serdes.String(), Serdes.String()))
                .mapValues((v) -> "|" + v + "|");
        builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .join(table, (l, r) -> l + " " + r)
                .to(outputTopic);

        properties = TestUtils.getStreamProperties(appId);
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input |table|", record.value());

        kafkaStreams.close();
    }


    /**
     * Simple change where we slightly change a State Store (by adding a mapValue to it)
     * This actually result in mismatch as the state store name change due to the automatically
     * generated name and we do not refresh the data...
     */
    @Test
    void simpleStateStoreChangeWithoutRefresh() throws ExecutionException, InterruptedException {
        String inputTopic = "simpleStateChangeNoRefresh-input";
        String outputTopic = "simpleStateChangeNoRefresh-output";
        String tableTopic = "simpleStateChangeNoRefresh-table";
        String appId = "simpleStateChangeNoRefresh";

        kafkaProducer.send(new ProducerRecord(tableTopic, "x", "table")).get();
        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "input")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .join(builder.table(tableTopic), (l, r) -> l + " " + r)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties(appId);
        System.out.println(builder.build().describe());
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input table", record.value());

        kafkaStreams.close();

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "input")).get();

        builder = new StreamsBuilder();
        KTable<String, String> table = builder
                .table(tableTopic, Materialized.with(Serdes.String(), Serdes.String()))
                .mapValues((v) -> "|" + v + "|");
        builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .join(table, (l, r) -> l + " " + r)
                .to(outputTopic);

        properties = TestUtils.getStreamProperties(appId);
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        System.out.println(builder.build().describe());
        kafkaStreams.start();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNull(record);
        kafkaStreams.close();
    }


    /**
     * Simple change where we slightly change a State Store (by adding a mapValue to it)
     * This time, we materialized our state store with a fixed name to avoid that the new
     * version of the topology generate a new name for it (and lost all previous data).
     *
     * Although we keep the data in the state from the previous version, the previous data
     * is not updated automatically to the new version, thus the new topology could contain
     * in the state previous and new version of the data
     */
    @Test
    void simpleStateStoreChangeWithoutRefreshAndMaterialized() throws ExecutionException, InterruptedException {
        String inputTopic = "simpleStateChangeNoRefresh-input";
        String outputTopic = "simpleStateChangeNoRefresh-output";
        String tableTopic = "simpleStateChangeNoRefresh-table";
        String tableChangelog = "simpleStateChangeNoRefresh-table-changelog";
        String appId = "simpleStateChangeNoRefresh";

        kafkaProducer.send(new ProducerRecord(tableTopic, "x", "table")).get();
        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "input")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic)
                .join(builder.table(tableTopic, Materialized.as(tableChangelog)), (l, r) -> l + " " + r)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties(appId);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input table", record.value());

        kafkaStreams.close();

        kafkaProducer.send(new ProducerRecord(inputTopic, "x", "input")).get();

        builder = new StreamsBuilder();
        KTable<String, String> table = builder
                .table(tableTopic,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as(tableChangelog))
                .mapValues((v) -> "|" + v + "|");
        builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .join(table, (l, r) -> l + " " + r)
                .to(outputTopic);

        properties = TestUtils.getStreamProperties(appId);
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input table", record.value());

        kafkaStreams.close();
    }

    /**
     * Adding a topic to the topology, this topic is actually processed first
     * as the messages inside this topic are older than in the other topic
     */
    @Test
    void addinginputTopic() throws ExecutionException, InterruptedException {
        String inputTopic1 = "addingTopic1-input";
        String inputTopic2 = "addingTopic2-input";
        String outputTopic = "addingTopic-output";
        String tableChangelog = "addingTopic-table-changelog";
        String appId = "addingTopic";

        kafkaProducer.send(new ProducerRecord(inputTopic1, "x", "input1")).get();
        kafkaProducer.send(new ProducerRecord(inputTopic2, "x", "input2")).get();

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic1)
                .to(outputTopic);

        Properties properties = TestUtils.getStreamProperties(appId);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        ConsumerRecord record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input1", record.value());

        kafkaStreams.close();

        kafkaProducer.send(new ProducerRecord(inputTopic1, "x", "input1")).get();

        builder = new StreamsBuilder();
        KTable<String, String> table = builder
                .table(inputTopic2,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as(tableChangelog));
        builder
                .stream(inputTopic1, Consumed.with(Serdes.String(), Serdes.String()))
                .join(table, (l, r) -> r)
                .to(outputTopic);

        properties = TestUtils.getStreamProperties(appId);
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        record = TestUtils.pollNextRecord(kafkaConsumer, outputTopic, 20000);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("input2", record.value());

        kafkaStreams.close();
    }
}