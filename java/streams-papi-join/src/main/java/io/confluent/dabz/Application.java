package io.confluent.dabz;

import io.confluent.dabz.examples.model.Customer;
import io.confluent.dabz.examples.model.Transaction;
import io.confluent.dabz.transformer.CustomerTransformer;
import io.confluent.dabz.transformer.MapperTransactionTransformerSupplier;
import io.confluent.dabz.transformer.TransactionTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;

public class Application {
    public static final String CUSTOMER_STORE_NAME = "customerStore";
    public static final String PENDING_TRANSACTION_NAME = "pendingTransactionStore";


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "papi-join");

        StreamsBuilder streamsBuilder = new StreamsBuilder();



        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(CUSTOMER_STORE_NAME),
                        Serdes.String(),
                        Serdes.String()
                )
        );
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(PENDING_TRANSACTION_NAME),
                        Serdes.String(),
                        SerdeFactory.createSerde()
                );


        KStream<String, Customer> customersRepartitionedStream = streamsBuilder
                .stream("customers", Consumed.with(Serdes.String(), SerdeFactory.<Customer>createSerde()))
                .flatMap((key, value) -> value.getLocalIds().stream()
                        .map((localId) -> new KeyValue<String, Customer>(localId.toString(), value))
                        .collect(Collectors.toList()))
                .repartition(Repartitioned.<String, Customer>as("customers-repartitioned-by-localIds").withKeySerde(Serdes.String()).withValueSerde(SerdeFactory.createSerde()));

        KStream<String, Transaction> transactionsRepartitionedStream = streamsBuilder
                .stream("transactions", Consumed.with(Serdes.String(), SerdeFactory.<Transaction>createSerde()))
                .map(((key, value) -> new KeyValue<>(value.getLocalCustomerId().toString(), value)))
                .repartition(Repartitioned.<String, Transaction>as("transactions-repartitioned-by-localIds").withKeySerde(Serdes.String()).withValueSerde(SerdeFactory.createSerde()));

        KStream<String, Transaction> transactionJoined = transactionsRepartitionedStream.
                transform(() -> new TransactionTransformer(), CUSTOMER_STORE_NAME, PENDING_TRANSACTION_NAME);

        KStream<String, Transaction> customerJoined = customersRepartitionedStream
                .flatTransform(() -> new CustomerTransformer(), CUSTOMER_STORE_NAME, PENDING_TRANSACTION_NAME);

        customerJoined.transform(
                new MapperTransactionTransformerSupplier((context, value) ->  null)
        );


        transactionJoined.merge(customerJoined).to("transactions-enriched", Produced.with(Serdes.String(), SerdeFactory.createSerde()));

        HashMap<String, ReadOnlyKeyValueStore> storesMap = new HashMap<>();
        streamsBuilder.globalTable("x", Materialized.as("x"));
        streamsBuilder.stream("topic")
                .mapValues((key, value) -> {
                    return storesMap.get("x").get(key);
                })
                .to("output");
        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
