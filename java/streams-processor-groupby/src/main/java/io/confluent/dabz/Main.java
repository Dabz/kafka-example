package io.confluent.dabz;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamy");

        Topology builder = new Topology();

        StoreBuilder<KeyValueStore<String, String>> aggregateStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ProcessorConcatToCsv.STATE_STORE_NAME),
                Serdes.String(),
                Serdes.String());

        builder.addSource("source", "processor-source")
                .addProcessor("process", () -> new ProcessorConcatToCsv(), "source")
                .addStateStore(aggregateStore, "process")
                .addSink("sink", "processor-sink", "process");

        KafkaStreams streams = new KafkaStreams(builder, properties);

        streams.start();
    }
}
