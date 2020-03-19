package io.confluent.dabz;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class OrderTransformer implements org.apache.kafka.streams.kstream.Transformer<String, String, org.apache.kafka.streams.KeyValue<String, String>> {
    private ProcessorContext localContext;
    private KeyValueStore stateStore;

    @Override
    public void init(ProcessorContext context) {
        localContext = context;
        stateStore = (KeyValueStore) context.getStateStore("line-state");
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        return getStringStringKeyValue(key, value);
    }

    private KeyValue<String, String> getStringStringKeyValue(String key, String value) {
        if (stateStore.get(key) != null) {
            String messages = stateStore.get(key).toString();
            localContext.forward(key, value);
            for (String s : messages.split(",")) {
                localContext.forward(key, s);
            }

            return null;
        }

        stateStore.put(key, "");
        return null;
    }

    @Override
    public void close() {

    }
}
