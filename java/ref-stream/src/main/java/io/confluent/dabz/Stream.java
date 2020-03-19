package io.confluent.dabz;

import kafka.server.KafkaServerStartable;
import org.apache.commons.math3.ode.nonstiff.EmbeddedRungeKuttaIntegrator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Stream implements Runnable {
    @Override
    public void run() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "coco");
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamProperties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, 1);
        streamProperties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, 1);
        streamProperties.put(StreamsConfig.pre, 1);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("ref-stream", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> lookout = streamsBuilder.table("ref-lookout", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ref-lookout-table")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

        KStream<String, String> map = stream.map((k, v) -> {
            return new KeyValue<>(k, "$$" + v + "$$");
        });
        KStream<String, String> join = map.join(lookout, (lstr1, lstr2) -> {
                    return lstr2 + " " + lstr1 + " " + lstr2;
                },
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "ref-join-changelog"));
        join.to("ref-output", Produced.with(Serdes.String(), Serdes.String()));
        KGroupedStream<String, String> groupBy = join.groupByKey();

        System.out.println(streamsBuilder.build().describe());

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
