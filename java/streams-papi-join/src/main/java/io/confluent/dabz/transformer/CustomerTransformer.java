package io.confluent.dabz.transformer;

import io.confluent.dabz.Application;
import io.confluent.dabz.examples.model.Customer;
import io.confluent.dabz.examples.model.PendingTransactions;
import io.confluent.dabz.examples.model.Transaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;


public class CustomerTransformer implements Transformer<String, Customer, Iterable<KeyValue<String, Transaction>>> {
    private KeyValueStore<String, String> customerStore;
    private KeyValueStore<String, PendingTransactions> pendingTransaction;

    @Override
    public void init(ProcessorContext context) {
        customerStore = (KeyValueStore<String, String>) context.getStateStore(Application.CUSTOMER_STORE_NAME);
        pendingTransaction = (KeyValueStore<String, PendingTransactions>) context.getStateStore(Application.PENDING_TRANSACTION_NAME);
    }

    @Override
    public Iterable<KeyValue<String, Transaction>> transform(String localCustomerId, Customer customer) {
        customerStore.put(localCustomerId, customer.getGlobalId().toString());

        PendingTransactions pendingTransactions = pendingTransaction.get(localCustomerId);
        if (pendingTransactions == null) {
            return null;
        }

        ArrayList<KeyValue<String, Transaction>> pendingTransactionList = new ArrayList<>();
        for (Transaction transaction : pendingTransactions.getTransactions()) {
            transaction.setGlobalId(customer.getGlobalId());
            pendingTransactionList.add(new KeyValue<>(customer.getGlobalId().toString(), transaction));
        }
        pendingTransaction.delete(localCustomerId);
        return pendingTransactionList;
    }

    @Override
    public void close() {

    }
}
