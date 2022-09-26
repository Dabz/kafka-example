package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;
import org.example.transformer.TimeBasedFilter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ts-based-filtering");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("ts"),
                Serdes.String(),
                Serdes.Long()
        ));

        streamsBuilder
                .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((key, value) -> explodeMerge(key, value))
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withName("input-exploded"))
                .transform(() -> new TimeBasedFilter(), "ts")
                .to("input-filtered", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Collection<KeyValue<String, String>> explodeMerge(String key, String value) {
        if (value.startsWith("MERGE")) {
            String[] split = value.split(" ");
            return Arrays.asList(
                    new KeyValue<>(split[1], "DELETE " + split[1]),
                    new KeyValue<>(split[2], "UPDATE " + split[2])
            );
        }
        return Collections.singleton(new KeyValue<>(key, value));
    }
}