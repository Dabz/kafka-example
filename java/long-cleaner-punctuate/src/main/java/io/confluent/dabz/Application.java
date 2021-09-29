package io.confluent.dabz;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Application {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        if (args.length > 0) {
            properties.load(new FileInputStream(args[0]));
        } else {
            properties.load(new Application().getClass().getResourceAsStream("/kafka.properties"));
        }
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        Application.addTopology(builder);
        Topology topology = builder.build();

        System.out.println(topology);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        kafkaStreams.start();
    }

    static void addTopology(StreamsBuilder builder) {
        builder.addStateStore(
                Stores.timestampedKeyValueStoreBuilder(
                        Stores.persistentTimestampedKeyValueStore("store"),
                        Serdes.String(),
                        Serdes.String()
                )
        );
        builder
                .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .transform(() -> new StoreWithSmartTTL(3600), "store")
                .to("output", Produced.with(Serdes.String(), Serdes.String()));
    }
}
