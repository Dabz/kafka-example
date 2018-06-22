package io.confluent.dabz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Stream implements Runnable {
    @Override
    public void run() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-stream-1");
        streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, "simple-stream-1");
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamProperties.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream1 = streamsBuilder.stream("join-stream-1", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> stream2 = streamsBuilder.stream("join-stream-2", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> join = stream1.join(stream2, (lstr1, lstr2) -> {
            return lstr1 + " " + lstr2;
        }, JoinWindows.of(TimeUnit.MINUTES.toMillis(5)), Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));
        join.to("join-stream", Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));    }
}
