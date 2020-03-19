package io.confluent.dabz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

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
        streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        streamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> builder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("line-state"),
                Serdes.String(),
                Serdes.String()
        );

        streamsBuilder.addStateStore(builder);

        KStream<String, String> line = streamsBuilder.stream("line", Consumed.with(Serdes.String(), Serdes.String()))
                .transform(() -> new LineTransformer(), "line-state");

        KStream<String, String> stream = handleError(streamsBuilder
                .stream("order", Consumed.with(Serdes.String(), Serdes.String()))
                .map((k, v) -> {
                    try {
                        return new KeyValue<>("toto", "toto");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        return new KeyValue<>("booh", "booh");
                    }
                }));

        stream.merge(line).to("order-enriched");

        System.out.println(streamsBuilder.build().describe());

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        kafkaStreams.setUncaughtExceptionHandler((e, v) -> {
            kafkaStreams.close();
        });

       Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public KStream<String, String> handleError(KStream str) {
        KStream[] branch = str.branch(
                (k, v) -> v.equals("booh"),
                (k, v) -> true
        );

        branch[0].to("error");
        return branch[1];
    }

    public static void main(String[] args) {
        new Stream().run();
    }
}
