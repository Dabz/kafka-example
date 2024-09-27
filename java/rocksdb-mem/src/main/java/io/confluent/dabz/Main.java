package io.confluent.dabz;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "rocksdb_mem");
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        properties.setProperty(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfig.class.getName());
        properties.setProperty(StreamsConfig.producerPrefix(ProducerConfig.BUFFER_MEMORY_CONFIG), String.valueOf(16*1024L*1024L));

        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<Long, byte[]>> store = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store"),
                Serdes.Long(),
                Serdes.ByteArray());
        builder.addStateStore(store);

        KTable<Object, Object> customer = builder.table("customer");
        builder.stream("order")
                .groupByKey()
                .aggregate()
                        .join(customer)


        builder.stream("test", Consumed.with(Serdes.Bytes(), Serdes.Bytes()))
                .transform(() -> new MyTransformer(), "store");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }
}
