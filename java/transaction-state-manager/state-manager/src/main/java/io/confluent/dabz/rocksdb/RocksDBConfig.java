package io.confluent.dabz.rocksdb;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.Map;

public class RocksDBConfig implements RocksDBConfigSetter {
    @Override
    public void setConfig(String s, Options options, Map<String, Object> map) {

    }

    @Override
    public void close(String s, Options options) {
        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
    }
}
