package io.confluent.dabz;

import io.confluent.dabz.model.ShakespeareKey;
import io.confluent.dabz.model.ShakespeareValue;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-streams-2");
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        streamProperties.put(StreamsConfig.NUM, 4);
        streamProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<ShakespeareKey, ShakespeareValue> stream = streamsBuilder.stream("shake");

        KStream<String, Long> countStream = stream
                .map((key, value) -> KeyValue.pair(key.getWork().toString(), value))
                .flatMapValues((key, value) -> Arrays.asList(value.getLine().toString().toLowerCase().replaceAll("[^a-zA-Z ]", "").split("[^a-zA-Z]+")))
                .groupBy((key, value) -> value, Serialized.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream();

        countStream.to("shake-count", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
