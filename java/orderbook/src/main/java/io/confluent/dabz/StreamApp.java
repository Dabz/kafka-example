package io.confluent.dabz;

import io.confluent.bootcamp.Order;
import io.confluent.dabz.serde.SerdeFactory;
import io.confluent.dabz.transformer.BookUpdateTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class StreamApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "orderbook");

        Thread thread = new Thread(() -> new Producer().run());
        thread.start();

        StreamsBuilder builder = new StreamsBuilder();
        StreamApp streamApp = new StreamApp();
        streamApp.buildTopology(builder);

        Topology build = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(build, properties);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static void buildTopology(StreamsBuilder builder) {
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constant.orderBookBuy),
                Serdes.String(),
                SerdeFactory.getSerde()
        ));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constant.orderBookSell),
                Serdes.String(),
                SerdeFactory.getSerde()
        ));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constant.orderLimitBookBuy),
                Serdes.String(),
                SerdeFactory.getSerde()
        ));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constant.orderLimitBookSell),
                Serdes.String(),
                SerdeFactory.getSerde()
        ));

        builder
                .stream(Constant.Order, Consumed.with(Serdes.String(), SerdeFactory.<Order>getSerde()))
                .transform(() -> new BookUpdateTransformer(), Constant.orderBookBuy, Constant.orderBookSell, Constant.orderLimitBookBuy, Constant.orderLimitBookSell);
    }
}
