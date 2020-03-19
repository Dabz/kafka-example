package io.confluent.dabz;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ProcessorConcatToCsv implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;
    private HashSet<String> dirtyQueue = new HashSet<String>();

    public static final String STATE_STORE_NAME = "daminou-state";

    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.kvStore = (KeyValueStore) processorContext.getStateStore(STATE_STORE_NAME);

        this.context.schedule(5000, PunctuationType.STREAM_TIME, (timestamp) -> {
            KeyValueIterator<String, String> keyValueIterator = this.kvStore.all();

            while (keyValueIterator.hasNext()) {
                KeyValue<String, String> keyValue = keyValueIterator.next();
                context.forward(keyValue.key, keyValue.value);
                this.kvStore.delete(keyValue.key);
            }
        });
    }

    public void process(String key, String value) {
        if (key == null) {
            key = "n/a";
        }

        value = String.format("bouga %s bouga", value);

        String oldValue = kvStore.get(key);

        if (oldValue == null) {
            kvStore.put(key, value);
        } else {
            kvStore.put(key, oldValue + "," + value);
        }

        synchronized(this) {
            dirtyQueue.add(key);
        }
    }

    public void punctuate(long l) {

    }

    public void close() {

    }
}
