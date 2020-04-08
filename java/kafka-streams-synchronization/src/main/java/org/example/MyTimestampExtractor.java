package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        String s = consumerRecord.value().toString();
        long x =  Long.parseLong(s.split("\\|")[0]);
        return x;
    }
}
