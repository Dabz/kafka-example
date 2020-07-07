package io.confluent.dabz;

import io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor;
import io.confluent.connect.replicator.offsets.ConsumerTimestampsWriterConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class SimpleWordCount {

    public static void main(String[] args) {
        Properties streamProperties = new Properties();
        streamProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "simple-stream-1");
        streamProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProperties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "5");
        streamProperties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        streamProperties.put(StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor");
        streamProperties.put(StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerTimestampsWriterConfig.TIMESTAMPS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        streamProperties.put(StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerTimestampsWriterConfig.TOPIC_WHITELIST_CONFIG, "simple-stream");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("simple-stream", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Long> countStream = stream
                .flatMapValues(value -> {
                    return Arrays.asList(value.toLowerCase().split("[^a-zA-Z]+"));
                })
                .groupBy((key, word) -> word)
                .count().toStream();

        countStream.to("simple-stream-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        System.out.println(streamsBuilder.build().describe());
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
