package io.confluent.dabz;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationTest {
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, String> inputTopic;
    private KeyValueStore<Object, ValueAndTimestamp<Object>> store;

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "XX");
        properties.setProperty(StoreWithSmartTTL.TimestampProviderConfig, MockTimestampProvider.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        Application.addTopology(builder);
        topologyTestDriver = new TopologyTestDriver(builder.build(), properties);
        inputTopic = topologyTestDriver.createInputTopic("input", new StringSerializer(), new StringSerializer());
        store = topologyTestDriver.getTimestampedKeyValueStore("store");
    }

    @AfterEach
    void close() {
        topologyTestDriver.close();
    }

    @Test
    void ensureSimple() {
        inputTopic.pipeInput("key1", "value");
        topologyTestDriver.advanceWallClockTime(Duration.ofDays(2));
        assertFalse(store.all().hasNext());
    }

    @Test
    void ensurePurgeIsCanceledIfTooSlow() {
        inputTopic.pipeInput("key1", "value");
        inputTopic.pipeInput("key2", "value");
        MockTimestampProvider.offset = 3600 * 24 * 2 * 1000 + 40000;
        // The first iteration should only delete one key as we add a 40s offset
        topologyTestDriver.advanceWallClockTime(Duration.ofDays(2));
        assertTrue(store.all().hasNext());

        // And the second iteration should finish the cleaning
        topologyTestDriver.advanceWallClockTime(Duration.ofMinutes(2));
        assertFalse(store.all().hasNext());
    }

}