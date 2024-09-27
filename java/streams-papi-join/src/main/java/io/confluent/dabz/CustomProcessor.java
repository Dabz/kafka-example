package io.confluent.dabz;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomProcessor implements FixedKeyProcessor<Object, Object, Object> {
    private FixedKeyProcessorContext<Object, Object> localContext;

    @Override
    public void init(FixedKeyProcessorContext<Object, Object> context) {
        FixedKeyProcessor.super.init(context);
        localContext = context;
    }

    @Override
    public void process(FixedKeyRecord<Object, Object> fixedKeyRecord) {
        InternalFixedKeyRecordFactory.create(new Record<>("test", "test", 0L));
        KeyValueStore<String, String> store = (KeyValueStore) localContext.getStateStore("x");

    }
}
