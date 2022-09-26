package org.example.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

public class TimeBasedFilter implements Transformer<String, String, KeyValue<String, String>> {
    private KeyValueStore<String, Long> store;
    private ProcessorContext localContext;

    @Override
    public void init(ProcessorContext processorContext) {
        store = processorContext.getStateStore("ts");
        localContext = processorContext;
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        Long oldTs = store.get(key);
        Long messageTs = localContext.timestamp();

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
