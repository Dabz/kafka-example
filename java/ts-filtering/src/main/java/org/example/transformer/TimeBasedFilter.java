package org.example.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.Constant;
import org.example.model.CustomerEnvelope;

public class TimeBasedFilter implements Transformer<String, CustomerEnvelope, KeyValue<String, CustomerEnvelope>> {
    private KeyValueStore<String, Long> store;
    private ProcessorContext localContext;

    @Override
    public void init(ProcessorContext processorContext) {
        store = processorContext.getStateStore(Constant.TS_STORE);
        localContext = processorContext;
    }

    @Override
    public KeyValue<String, CustomerEnvelope> transform(String key, CustomerEnvelope value) {
        Long oldTs = store.get(key);
        Long messageTs = value.getTs().toEpochMilli();

        if (oldTs == null) {
            store.put(key, messageTs);
            return new KeyValue<>(key, value);
        }

        if (messageTs > oldTs) {
            store.put(key, messageTs);
            return new KeyValue<>(key, value);
        }

        return null;
    }

    @Override
    public void close() {

    }
}
