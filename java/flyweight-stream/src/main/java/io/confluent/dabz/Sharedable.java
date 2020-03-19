package io.confluent.dabz;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.TypeElement;
import java.util.Set;

public class Sharedable implements Transformer<String, String, String> {
    private KeyValueStore kvStore;
    private ProcessorContext context;
    final static public String STATE_STORE_NAME = "flyweight-shared";

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.kvStore = (KeyValueStore) processorContext.getStateStore(STATE_STORE_NAME);
    }

    @Override
    public String transform(String hash, String value) {
        int sequence = 0;
        KeyValueIterator<Integer, String> valueIterator = this.kvStore.range(hash, );

            while (valueIterator.hasNext()) {
                KeyValue<Integer, String> next = valueIterator.next();
                if (next.value.equals(value)) {
                    return next.key;
                }
                sequence = next.key - hash;
            }

            sequence += 1;

            return value;
    }

    @Override
    public void close() {

    }
}
