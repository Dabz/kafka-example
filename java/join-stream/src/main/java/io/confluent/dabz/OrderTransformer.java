package io.confluent.dabz;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class OrderTransformer implements org.apache.kafka.streams.kstream.Transformer<String, String, org.apache.kafka.streams.KeyValue<String, String>> {
    private ProcessorContext localContext;
    private KeyValueStore stateStore;

    @Override
    public void init(ProcessorContext context) {
        localContext = context;
        KeyValueStore lol = (KeyValueStore) context.getStateStore("lol");

        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (ts) -> {
            System.out.println(context.timestamp());
        });
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        try {
            return doTransform(key, value);
        } finally {
            return null;
        }
    }

    public KeyValue<String, String> doTransform(String key, String value) {
        localContext.headers().add(new RecordHeader("offset", "500".getBytes(StandardCharsets.UTF_8)));
        localContext.headers().headers("lol");
        localContext.offset();
        return new KeyValue<>(key, value);
    }

    private KeyValue<String, String> getStringStringKeyValue(String key, String value) {
        return null;
    }

    @Override
    public void close() {

    }
}
