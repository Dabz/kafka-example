package io.confluent.dabz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) throws Exception {
        var props = new Properties();
        if (args.length > 0) {
            props.load(new FileReader(args[0]));
        } else {
            props.load(this.getClass().getResourceAsStream("/kafka.properties"));
        }

        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        props.setProperty(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .stream("input")
                .map((k, v) -> new KeyValue<>(k, v))
                .to("output");

        Topology topology = streamsBuilder.build();
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));
    }
}
