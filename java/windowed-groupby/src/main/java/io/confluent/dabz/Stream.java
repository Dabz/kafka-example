package io.confluent.dabz;

import io.confluent.dabz.model.VisitEventAggregatedValue;
import io.confluent.dabz.model.VisitEventKey;
import io.confluent.dabz.model.VisitEventValue;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.WindowStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Stream implements Runnable {
    @Override
    public void run() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "windows-groupby");
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        streamProperties.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        streamProperties.put(StreamsConfig.PRODUCER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        StreamsBuilder builder = new StreamsBuilder();

        SpecificAvroSerde avroSerde = new SpecificAvroSerde();
        avroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"), false);

        KStream<VisitEventKey, VisitEventValue> visits = builder.stream("visits");
        KTable<Windowed<String>, Long> count_10_seconds = visits
                .groupBy((key, value) -> key.getPage().toString(), Serialized.with(Serdes.String(), avroSerde))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(10)))
                .count(Materialized.as("visits-windows-10sec"));

        KStream<VisitEventAggregatedValue, Long> stream = count_10_seconds.toStream((k, v) -> {
            VisitEventAggregatedValue key = new VisitEventAggregatedValue();
            key.setPage(k.key());
            key.setDate(k.window().start());
            key.setDuration(10);
            return key;
        });

        stream.to("visits_10_seconds", Produced.with(avroSerde, Serdes.Long()));

        ArrayList<Object> objects = new ArrayList<>();

        KTable<Windowed<String>, Long> count_60_seconds = visits
                .groupBy((VisitEventKey key, VisitEventValue value) -> {
                    return key.getPage().toString();
                }, Serialized.with(Serdes.String(), avroSerde))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(60)))
                .count(Materialized.as("visits-windows-60sec"));

        KStream<VisitEventAggregatedValue, Long> stream_60 = count_60_seconds.toStream((k, v) -> {
            VisitEventAggregatedValue key = new VisitEventAggregatedValue();
            key.setPage(k.key());
            key.setDate(k.window().start());
            key.setDuration(60);
            return key;
        });

        stream_60.to("visits_60_seconds", Produced.with(avroSerde, Serdes.Long()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProperties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
