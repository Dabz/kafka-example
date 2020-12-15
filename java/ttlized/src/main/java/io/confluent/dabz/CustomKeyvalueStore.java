package io.confluent.dabz;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.ietf.jgss.GSSContext;

import java.time.Duration;
import java.util.Properties;

public class CustomKeyvalueStore {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ttlizedmyktable-custom-supplier");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> users = builder.table("users",
                Consumed.with(Serdes.String(), Serdes.String()).withName("USER-KTABLE-SOURCE"),
                Materialized.as(new WindowedKeyValueStoreSupplier("users-ktable-kt", 10_000L)));

        builder.stream("user-events", Consumed.with(Serdes.String(), Serdes.String()))
                .join(users, (event, user) -> {
                    Gson gson = new Gson();
                    JsonObject eventObject = gson.fromJson(event, JsonObject.class);
                    JsonObject userObject = gson.fromJson(user, JsonObject.class);
                    eventObject.add("user", userObject);
                    return gson.toJson(eventObject);
                })
                .to("user-events-enriched", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.start();
    }
}
