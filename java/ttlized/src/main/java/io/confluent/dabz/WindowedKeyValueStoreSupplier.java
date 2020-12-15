package io.confluent.dabz;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;

import java.time.Instant;
import java.util.Date;
import java.util.List;

public class WindowedKeyValueStoreSupplier implements KeyValueBytesStoreSupplier {
    private final WindowStore<Bytes, byte[]> store;

    public WindowedKeyValueStoreSupplier(String name, long retentionMs) {
        this.store = new RocksDbWindowBytesStoreSupplier(name, retentionMs, retentionMs / 2, retentionMs / 2, false, true).get();
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new KeyValueStore<Bytes, byte[]>() {
            @Override
            public void put(Bytes key, byte[] value) {
                store.put(key, value, new Date().getTime());
            }

            @Override
            public byte[] putIfAbsent(Bytes key, byte[] value) {
                store.put(key, value, new Date().getTime());
                return value;
            }

            @Override
            public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
                for (KeyValue<Bytes, byte[]> entry : entries) {
                    this.put(entry.key, entry.value);
                }
            }

            @Override
            public byte[] delete(Bytes key) {
                this.put(key, null);
                return new byte[]{};
            }

            @Override
            public String name() {
                return store.name();
            }

            @Override
            public void init(ProcessorContext context, StateStore root) {
                store.init(context, root);
            }

            @Override
            public void flush() {
                store.flush();
            }

            @Override
            public void close() {
                store.close();
            }

            @Override
            public boolean persistent() {
                return store.persistent();
            }

            @Override
            public boolean isOpen() {
                return store.isOpen();
            }

            @Override
            public byte[] get(Bytes key) {
                WindowStoreIterator<byte[]> iterator = store.fetch(key, Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE));
                KeyValue<Long, byte[]> lastEntry = null;

                while (iterator.hasNext()) {
                    lastEntry = iterator.next();
                }
                if (lastEntry == null) {
                    return null;
                }

                iterator.close();
                return lastEntry.value;
            }

            @Override
            public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
                KeyValueIterator<Windowed<Bytes>, byte[]> fetch = store.fetch(from, to, Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE));
                return keyValueIteratorFromWindowIterator(fetch);
            }

            @Override
            public KeyValueIterator<Bytes, byte[]> all() {
                return keyValueIteratorFromWindowIterator(store.all());
            }

            @Override
            public long approximateNumEntries() {
                System.err.println("Not implemented");
                return 0;
            }
        };
    }

    @Override
    public String metricsScope() {
        return null;
    }


    public KeyValueIterator<Bytes, byte[]> keyValueIteratorFromWindowIterator(KeyValueIterator<Windowed<Bytes>, byte[]> fetch) {
        return new KeyValueIterator<Bytes, byte[]>() {
            @Override
            public void close() {
                fetch.close();
            }

            @Override
            public Bytes peekNextKey() {
                return fetch.peekNextKey().key();
            }

            @Override
            public boolean hasNext() {
                return fetch.hasNext();
            }

            @Override
            public KeyValue<Bytes, byte[]> next() {
                KeyValue<Windowed<Bytes>, byte[]> next = fetch.next();
                return new KeyValue<>(next.key.key(), next.value);
            }
        };
    }
}
