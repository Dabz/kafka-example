package io.confluent.dabz;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.KTableImpl;

import java.util.Properties;

public class CustomProcessorThatForward {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ttlizedmyktable");
        properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> users = builder.table("users",
                Consumed.with(Serdes.String(), Serdes.String()).withName("USER-KTABLE-SOURCE"),
                Materialized.as("user-ktable"));

        Topology topology = builder.build();
        topology = topology.addProcessor("ttlized-ktable", () -> new KTableTombstoneGenerator<String, String>(users.queryableStoreName(), "USER-KTABLE-SOURCE-source"), "USER-KTABLE-SOURCE-source");
        topology.connectProcessorAndStateStores("ttlized-ktable", users.queryableStoreName());
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.start();
    }
}
