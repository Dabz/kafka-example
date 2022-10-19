package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;

import java.time.Duration;
import java.util.Properties;

public class Stream {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "hello-world");
        properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        properties.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        properties.setProperty(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "60000");
        properties.setProperty(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "100");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        if (args.length > 0) {
            properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, args[0]);
        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var table = streamsBuilder.stream("table");
        streamsBuilder
                .stream("stream")
                .join(table, (eSt, eTb) -> eSt.toString() + eTb.toString(), JoinWindows.of(Duration.ofSeconds(5)))
                .to("output");

        var kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}