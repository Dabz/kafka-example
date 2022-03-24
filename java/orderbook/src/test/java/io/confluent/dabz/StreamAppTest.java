package io.confluent.dabz;

import io.confluent.bootcamp.LimitBookEntry;
import io.confluent.bootcamp.Order;
import io.confluent.dabz.serde.SerdeFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class StreamAppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> orderTopic;
    private KeyValueStore<String, LimitBookEntry> limitBuy;
    private KeyValueStore<String, LimitBookEntry> limitSell;
    private KeyValueStore<String, Order> bookBuy;
    private KeyValueStore<String, Order> bookSell;

    @BeforeEach
    void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        StreamApp.buildTopology(builder);
        Topology topology = builder.build();
        System.out.println(topology.describe());
        testDriver = new TopologyTestDriver(topology);

        orderTopic = testDriver.createInputTopic(Constant.Order, new StringSerializer(), SerdeFactory.<Order>getSerde().serializer());
        limitBuy = testDriver.getKeyValueStore(Constant.orderLimitBookBuy);
        limitSell = testDriver.getKeyValueStore(Constant.orderLimitBookSell);
        bookBuy = testDriver.getKeyValueStore(Constant.orderBookBuy);
        bookSell = testDriver.getKeyValueStore(Constant.orderBookSell);
    }

    @AfterEach
    void close() {
        testDriver.close();
    }

    Order buildOrder(String symbol, String type, Double price, Long quantiy) {
        return Order.newBuilder()
                .setId(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE)
                .setInstrument(symbol)
                .setPrice(price)
                .setQuantity(quantiy)
                .setType(type)
                .build();
    }

    @Test
    void ensureStoreIsUpdated() {
        String symbol = "TRUMP-USD";
        orderTopic.pipeInput(symbol, buildOrder(symbol, "BUY", 1.0, 10L));
        orderTopic.pipeInput(symbol, buildOrder(symbol, "BUY", 1.0, 1L));
        orderTopic.pipeInput(symbol, buildOrder(symbol, "BUY", 1.3, 3L));

        orderTopic.pipeInput(symbol, buildOrder(symbol, "SELL", 10.0, 3L));
        orderTopic.pipeInput(symbol, buildOrder(symbol, "SELL", 5.0, 2L));
        orderTopic.pipeInput(symbol, buildOrder(symbol, "SELL", 5.0, 1L));

        int count = 0;
        try (var iterator = bookBuy.all()) {
            while (iterator.hasNext()) {
                iterator.next();
                count += 1;
            }
        }
        assertEquals(3, count);

        count = 0;
        try (var iterator = bookSell.all()) {
            while (iterator.hasNext()) {
                iterator.next();
                count += 1;
            }
        }
        assertEquals(3, count);

        count = 0;
        try (var iterator = limitBuy.all()) {
            while (iterator.hasNext()) {
                var next = iterator.next();
                count += 1;
                if (next.value.getPrice() == 1.0) {
                    assertEquals(2, next.value.getOrderCount());
                }
            }
        }
        assertEquals(2, count);

        count = 0;
        try (var iterator = limitSell.all()) {
            while (iterator.hasNext()) {
                var next = iterator.next();
                count += 1;
                if (next.value.getPrice() == 5.0) {
                    assertEquals(2, next.value.getOrderCount());
                }
            }
        }
        assertEquals(2, count);
    }

}