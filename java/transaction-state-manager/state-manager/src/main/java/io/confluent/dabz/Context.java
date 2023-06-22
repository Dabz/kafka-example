package io.confluent.dabz;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Context {
    static private Map<String, String> configuration = new HashMap<>();
    static private Producer<String, byte[]> producer = null;
    static Logger logger = LoggerFactory.getLogger(Context.class.getName());
    static private KafkaStreams kafkaStreams = null;
    static private HostInfo hostInfo = null;
    private static AdminClient adminClient;

    public static Map<String, String> getConfiguration() {
        return configuration;
    }

    public static void sendMessageToDLQ(String dlqTopic, Exception e, byte[] value) throws ExecutionException, InterruptedException {
        sendMessageToDLQ(dlqTopic, e, value, "unknown", -1, -1);
    }

    public static synchronized void sendMessageToDLQ(String dlqTopic, Exception e, byte[] value, String topic, int partition, long offset) throws ExecutionException, InterruptedException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        logger.error("Sending message to the DLQ. Input topic {}, partition: {}, offset: {}.\nException: {}\nStacktrace: {}",
                topic, partition, offset, e.getMessage(), sw);

        if (producer == null) {
            logger.info("Creating DLQ Kafka Producer");
            Properties producerProperties = new Properties();
            configuration.forEach((k, v) -> producerProperties.put(k, v));
            producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            producer = new KafkaProducer<String, byte[]>(producerProperties);
        }

        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(dlqTopic, e.getMessage(), value);
        record.headers().add(new RecordHeader("topic", new StringSerializer().serialize("", topic)));
        record.headers().add(new RecordHeader("partition", new IntegerSerializer().serialize("", partition)));
        record.headers().add(new RecordHeader("offset", new LongSerializer().serialize("", offset)));
        record.headers().add(new RecordHeader("exception", new StringSerializer().serialize("", e.getMessage())));
        record.headers().add(new RecordHeader("stacktrace", new StringSerializer().serialize("", sw.toString())));

        producer.send(record).get();
    }

    public static AdminClient getAdminClient() {
        if (adminClient != null) {
            return adminClient;
        }

        Properties properties = new Properties();
        configuration.forEach((k, v) -> properties.put(k, v));
        adminClient = KafkaAdminClient.create(properties);
        return adminClient;
    }

    public static HostInfo getCurrentHost() {
        if (hostInfo != null)
            return hostInfo;
        String applicationServer = configuration.getOrDefault(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");
        String[] split = applicationServer.split(":");
        if (split.length < 2) {
            throw new RuntimeException("Invalid application.server configuration");
        }
        hostInfo = new HostInfo(split[0], Integer.valueOf(split[1]));
        return hostInfo;
    }

    public static void setProducer(Producer<String, byte[]> producer) {
        Context.producer = producer;
    }

    public static KafkaStreams getKafkaStreams() {
        return kafkaStreams;
    }

    public static void setKafkaStreams(KafkaStreams kafkaStreams) {
        Context.kafkaStreams = kafkaStreams;
    }

    public static void setAdminClient(AdminClient adminClient) {
        Context.adminClient = adminClient;
    }
}
