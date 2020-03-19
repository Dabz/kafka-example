package io.confluent.dabz;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CountByTimeAndNumber implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;

    private HashSet<String> dirtyQueue = new HashSet<String>();

    public static final String STATE_STORE_NAME = "count-store";
    public static final Integer MAX_NUMBER_OF_MESSAGES = 5;
    public Integer numberOfMessage = 0;
    private KeyValueStore accountKeyStore;

    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.kvStore = (KeyValueStore) processorContext.getStateStore(STATE_STORE_NAME);
        accountKeyStore = (KeyValueStore) processorContext.getStateStore(STATE_STORE_NAME);

        this.context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            System.out.println(timestamp);
            flush();
        });
    }

    public void process(String key, String value) {
        if (key == null) {
            key = "";
        }
        kvStore.put(key, value);
        numberOfMessage += 1;

        if (numberOfMessage >= MAX_NUMBER_OF_MESSAGES) {
            flush();
        }
    }

    private void flush() {
        numberOfMessage = 0;
        KeyValueIterator<String, String> keyValueIterator = this.kvStore.all();
        while (keyValueIterator.hasNext()) {
            KeyValue<String, String> keyValue = keyValueIterator.next();
            context.forward(keyValue.key, keyValue.value);
            this.kvStore.delete(keyValue.key);
        }
    }

    public void punctuate(long l) {

    }

    public void close() {

    }
}
