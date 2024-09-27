package io.confluent.dabz.transformer;

import io.confluent.dabz.Application;
import io.confluent.dabz.examples.model.PendingTransactions;
import io.confluent.dabz.examples.model.Transaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;

public class TransactionTransformer implements Transformer<String, Transaction, KeyValue<String, Transaction>> {
    private KeyValueStore<String, String> customerStore;
    private KeyValueStore<String, PendingTransactions> pendingTransaction;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        customerStore = (KeyValueStore<String, String>) context.getStateStore(Application.CUSTOMER_STORE_NAME);
        pendingTransaction = (KeyValueStore<String, PendingTransactions>) context.getStateStore(Application.PENDING_TRANSACTION_NAME);

        this.context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME,(ts) -> {
            try (KeyValueIterator<String, PendingTransactions> all = pendingTransaction.all()) {
                while (all.hasNext()) {
                    KeyValue<String, PendingTransactions> next = all.next();
                    if (true) { //check if expired
                        pendingTransaction.delete(next.key);
                    }
                }
            }
        })
    }

    @Override
    public KeyValue<String, Transaction> transform(String localCustomerId, Transaction transaction) {
        var customerGlobalId = customerStore.get(transaction.getLocalCustomerId().toString());
        if (customerGlobalId != null) {
            transaction.setGlobalId(customerGlobalId);
            return new KeyValue<>(customerGlobalId, transaction);
        }

        PendingTransactions pendingTransactions = pendingTransaction.get(localCustomerId);
        if (pendingTransactions == null) {
            pendingTransactions = PendingTransactions.newBuilder()
                    .setTransactions(new ArrayList<>())
                    .build();
        }
        pendingTransactions.getTransactions().add(transaction);
        pendingTransactions.put(localCustomerId, pendingTransactions);

        return null;
    }

    @Override
    public void close() {

    }
}
