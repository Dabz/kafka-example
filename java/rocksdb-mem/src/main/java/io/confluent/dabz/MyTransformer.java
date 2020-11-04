package io.confluent.dabz;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Date;
import java.util.Random;

public class MyTransformer implements Transformer<Bytes, Bytes, KeyValue<Bytes, Bytes>> {
    private ProcessorContext localContext;
    private KeyValueStore store;

    @Override
    public void init(ProcessorContext context) {
        localContext = context;
        store = (KeyValueStore) localContext.getStateStore("store");

        Random random = new Random();
        localContext.schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME, (ts) -> {
            byte lol[] = new byte[512 * 1024];
            random.nextBytes(lol);
            store.put(new Date().getTime(), lol);
        });
    }

    @Override
    public KeyValue<Bytes, Bytes> transform(Bytes key, Bytes value) {
        return null;
    }

    @Override
    public void close() {

    }
}
