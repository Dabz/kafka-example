package io.confluent.dabz;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class LineTransformer implements org.apache.kafka.streams.kstream.Transformer<String, String, org.apache.kafka.streams.KeyValue<String, String>> {
    private KeyValueStore lineState;
    private ProcessorContext localContext;

    @Override
    public void init(ProcessorContext context) {
        lineState = (KeyValueStore) context.getStateStore("line-state");
        localContext = context;
   }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        if (lineState.get(key) != null) {
            if (lineState.get(key).equals("")) {
                return new KeyValue<>(key, value);
            }
        }

        if (lineState.get(key) != null && ! lineState.get(key).equals("")) {
            String previousLine = lineState.get(key).toString();
            previousLine += "," + value;
            lineState.put(key, previousLine);
            return null;
        }

        lineState.put(key, value);
        return null;
    }

    @Override
    public void close() {

    }
}
