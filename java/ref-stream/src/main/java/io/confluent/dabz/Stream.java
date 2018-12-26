package io.confluent.dabz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Stream implements Runnable {
    @Override
    public void run() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ref-stream");
        streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, "ref-stream");
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamProperties.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG, "all");
        streamProperties.put(StreamsConfig.EXACTLY_ONCE, true);
        streamProperties.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("ref-stream", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> lookout = streamsBuilder.stream("ref-lookout", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> mapValues = lookout.map((k, v) -> {
            return new KeyValue<>(k, String.format("??? %s ???", v));
        });
        KStream<String, String> flatMapValues = mapValues.flatMapValues(v -> {
            return Arrays.asList(v.split(","));
        });
        KStream<String, String> join = stream.join(flatMapValues
                , (lstr1, lstr2) -> {
            return lstr2 + " " + lstr1 + " " + lstr2;
        }, JoinWindows.of(TimeUnit.HOURS.toMillis(10)), Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));
        join.to("ref-output", Produced.with(Serdes.String(), Serdes.String()));
        KGroupedStream<String, String> groupBy = join.groupByKey();


        System.out.println(streamsBuilder.build().describe());

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
