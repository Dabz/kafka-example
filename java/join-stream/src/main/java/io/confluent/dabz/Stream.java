package io.confluent.dabz;

import io.confluent.shaded.com.google.gson.Gson;
import io.confluent.shaded.com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Stream implements Runnable {
    @Override
    public void run() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-stream-1");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);

        StreamsBuilder streamsBuilder  = new StreamsBuilder();

        KTable<String, String> users = streamsBuilder.table("users", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("XXXX").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        streamsBuilder
                .stream("events", Consumed.with(Serdes.String(), Serdes.String()))
                .join(users, (user, event) -> {
                    Gson gson = new Gson();
                    JsonObject userObject = gson.fromJson(user, JsonObject.class);
                    JsonObject eventObject = gson.fromJson(event, JsonObject.class);
                    eventObject.add("user", userObject);
                    return gson.toJson(eventObject);
                }).to("event-enriched", Produced.with(Serdes.String(), Serdes.String()));

        System.out.println(streamsBuilder.build().describe());
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static void main(String[] args) {
        new Stream().run();
    }
}
