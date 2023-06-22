package io.confluent.dabz;

import io.confluent.dabz.model.Transaction;
import io.confluent.dabz.rocksdb.RocksDBConfig;
import io.confluent.dabz.transformers.TransactionStateProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.util.Properties;

public class StreamsApp {
    public static void main(String[] args) throws Exception {
        StreamsApp streamsApp = new StreamsApp();
        streamsApp.start(args);
    }

    private void start(String[] args) throws Exception {
        Properties properties = new Properties();
        if (args.length > 0) {
            properties.load(new FileInputStream(args[0]));
        } else {
            properties.load(this.getClass().getResourceAsStream("/kafka.properties"));
        }

        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-state-manager");
        properties.setProperty(StreamsConfig.CLIENT_ID_CONFIG, "transaction-state-manager");
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfig.class.getName());
        properties.forEach((key, value) -> Context.getConfiguration().put(key.toString(), value.toString()));

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("transactions-state"),
                Serdes.String(),
                SerdeGenerator.getSerde()
        ));

        streamsBuilder
                .stream("transactions", Consumed.with(Serdes.String(), SerdeGenerator.<Transaction>getSerde()))
                .process(() -> new TransactionStateProcessor(), "transactions-state")
                .to("transactions-state", Produced.with(Serdes.String(), SerdeGenerator.getSerde()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}