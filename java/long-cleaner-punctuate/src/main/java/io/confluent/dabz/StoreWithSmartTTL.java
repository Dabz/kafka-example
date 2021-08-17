package io.confluent.dabz;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.util.Date;

public class StoreWithSmartTTL implements org.apache.kafka.streams.kstream.Transformer<String, String, KeyValue<String, String>> {
    public static final String TimestampProviderConfig = "dabz.timestamp.provider";

    private final long storeTimeoutMs;
    private Integer transactionTimeoutMs;
    private KeyValueStore<String, ValueAndTimestamp> store;
    private ProcessorContext localContext;
    private final String HIGHEST_STRING = new String(new char[] {255});
    private final String LOWEST_STRING = new String(new char[] { 0 });
    String cleanerCheckpoint = LOWEST_STRING;
    TimestampProvider timestampProvider;


    public StoreWithSmartTTL(long storeTimeout) {
        this.storeTimeoutMs = storeTimeout;
    }

    @Override
    public void init(ProcessorContext context) {
        store = context.getStateStore("store");
        localContext = context;
        transactionTimeoutMs = (Integer) localContext.appConfigs().getOrDefault(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 60000);
        String timestampProviderClass = (String) localContext.appConfigs().getOrDefault(TimestampProviderConfig, TimestampProvider.class.getName());
        try {
            this.timestampProvider = (TimestampProvider) Class.forName(timestampProviderClass).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, (ts) -> {
            long startTime = ts;
            try (var allIterator = store.range(cleanerCheckpoint, HIGHEST_STRING)) {
                while (allIterator.hasNext()) {
                    KeyValue<String, ValueAndTimestamp> next = allIterator.next();
                    if (startTime - next.value.timestamp() > storeTimeoutMs) {
                        store.delete(next.key);
                    }
                    long currentTime = this.timestampProvider.getTimestamp();
                    // If it takes too much time, we interrupt the purging process and keep
                    // track of the latest key that has been deleted
                    if (currentTime - startTime > (transactionTimeoutMs * 0.5)) {
                        cleanerCheckpoint = next.key;
                        break;
                    }
                }
                // If we were able to complete the whole purging job, we reset the checkpoint
                // to the lowest possible string ('\0')
                cleanerCheckpoint = LOWEST_STRING;
            }

        });
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        store.put(key, ValueAndTimestamp.make(value, new Date().getTime()));

        return null;
    }

    @Override
    public void close() {

    }
}
