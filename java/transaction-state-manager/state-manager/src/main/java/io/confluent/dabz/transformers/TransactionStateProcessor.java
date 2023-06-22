package io.confluent.dabz.transformers;

import io.confluent.dabz.model.RegulatorStateEnum;
import io.confluent.dabz.model.Transaction;
import io.confluent.dabz.model.TransactionState;
import io.confluent.dabz.model.TransactionStateEnum;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.GregorianCalendar;

public class TransactionStateProcessor implements Processor<String, Transaction, String, TransactionState> {
    private KeyValueStore<String, TransactionState> store;
    private ProcessorContext<String, TransactionState> localContext;

    @Override
    public void init(ProcessorContext<String, TransactionState> context) {
        Processor.super.init(context);
        this.store = context.getStateStore("transactions-state");
        this.localContext = context;
    }

    @Override
    public void process(Record<String, Transaction> record) {
        String key = record.key();
        var lei = key.split("#")[0];
        var txnRef = key.split("#")[1];

        var previousState = this.store.get(key);
        if (previousState == null) {
            previousState = TransactionState.newBuilder()
                    .setREGULATORID(record.value().getREGULATORID())
                    .setREGULATORSTATE(RegulatorStateEnum.NEW)
                    .setTRANSACTIONSTATE(TransactionStateEnum.NEW)
                    .setTRANSACTIONID(record.value().getTRANSACTIONID())
                    .setTRANSACTIONCOUNT(1)
                    .build();
        } else {
            previousState.setTRANSACTIONCOUNT(previousState.getTRANSACTIONCOUNT() + 1);
        }
        store.put(key, previousState);

        this.localContext.forward(new Record<>(key, previousState, localContext.currentStreamTimeMs()));
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
