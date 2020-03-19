package io.confluent.dabz;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import scala.reflect.internal.TypeDebugging;

import java.util.Properties;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "count");

        Topology builder = new Topology();

        StoreBuilder<KeyValueStore<String, String>> aggregateStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(CountByTimeAndNumber.STATE_STORE_NAME),
                Serdes.String(),
                Serdes.String());

        builder.addSource("source", "count-source")
                .addProcessor("process", () -> new CountByTimeAndNumber(), "source")
                .addStateStore(aggregateStore, "process")
                .addSink("sink", "count-sink", "process");


        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();
    }
}
