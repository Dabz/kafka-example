package io.confluent.dabz;

import io.confluent.dabz.model.ShakespeareKey;
import io.confluent.dabz.model.ShakespeareValue;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.JsonProperties;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "archlinux.local:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-streams-2");
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamProperties.put("deadletterqueue.record.builder", "blahblah");
        streamProperties.put("deadletterqueue.record.topic.name", "myDLQ");
        streamProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://archlinux.local:8081");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<Object, Object> buildings = streamsBuilder.table("buildings", Materialized.as("buildings-table"));
        streamsBuilder.stream("appartments")
                .filter((key, value) -> true)
                .selectKey((key, value) -> value)
                .repartition(Repartitioned.as("appartments-with-building-key"))
                .leftJoin(buildings, (app, bui) -> {
                    return  app.toString() + bui.toString();
                }).to("appartments-with-buidling");

        System.out.println(streamsBuilder.build().describe());




        KStream<ShakespeareKey, ShakespeareValue> stream = streamsBuilder.stream("shake");

        KTable<Object, Object> users = streamsBuilder.table("users");
        streamsBuilder.stream("purchaseOrders")
                .join(users, (left, right) -> {
                    return right;
                })
                .to("poEnriched");


        KStream<String, Long> countStream =
                .mapValues((k, v) -> ImmutablePair.of("tes", v))
                .mapValues((k, v) -> { if (1 == 1) throw new NullPointerException(); return v; } )
                .mapValues((k, v) -> v.getValue())
                .map((key, value) -> KeyValue.pair(key.getWork().toString(), value))
                .flatMapValues((key, value) -> Arrays.asList(value.getLine().toString().toLowerCase().replaceAll("[^a-zA-Z ]", "").split("[^a-zA-Z]+")))
                .toStream();*/



        // Interactive Query

        // countStream.to("shake-count", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
