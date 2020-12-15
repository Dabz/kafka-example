package io.confluent.dabz;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.internals.NamedInternal;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.Duration;

public class KTableTombstoneGenerator<K, V> implements Processor<K, V> {
    private final String storeName;
    private final String destChildName;

    public KTableTombstoneGenerator(String ktableStoreName, String dchild) {
        this.storeName = ktableStoreName;
        this.destChildName = dchild;
    }

    public void init(ProcessorContext context) {
        KeyValueStore stateStore = (KeyValueStore) context.getStateStore(storeName);

        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (ts) -> {
            KeyValueIterator all = stateStore.all();
            while (all.hasNext()) {
                KeyValue<String, Object> next = (KeyValue<String, Object>) all.next();
                if (Integer.parseInt(next.key) > 10) {
                    stateStore.delete(next.key);
                }
            }
        });
    }

    @Override
    public void process(K key, V value) {

    }

    public void close() {

    }
}
