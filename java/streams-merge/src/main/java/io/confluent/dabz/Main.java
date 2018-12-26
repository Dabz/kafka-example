package io.confluent.dabz;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Thread producerThread = new Thread(new Producer());

        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-merge");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, Object> merge = builder.stream("merge-1").merge(builder.stream("merge-2"));
        merge.to("merge-result");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProperties);


        kafkaStreams.cleanUp();
        kafkaStreams.start();

        producerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
