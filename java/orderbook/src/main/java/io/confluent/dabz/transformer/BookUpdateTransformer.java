package io.confluent.dabz.transformer;

import io.confluent.bootcamp.LimitBookEntry;
import io.confluent.bootcamp.Order;
import io.confluent.dabz.Constant;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class BookUpdateTransformer implements Transformer<String, Order, KeyValue<String, Order>> {
    private KeyValueStore<String, Order> orderBookBuy;
    private KeyValueStore<String, Order> orderBookSell;
    private KeyValueStore<String, LimitBookEntry> limitBookBuy;
    private KeyValueStore<String, LimitBookEntry> limitBookSell;

    @Override
    public void init(ProcessorContext context) {
        orderBookBuy = context.getStateStore(Constant.orderBookBuy);
        orderBookSell = context.getStateStore(Constant.orderBookSell);
        limitBookBuy = context.getStateStore(Constant.orderLimitBookBuy);
        limitBookSell = context.getStateStore(Constant.orderLimitBookSell);
    }

    @Override
    public KeyValue<String, Order> transform(String key, Order order) {
        updateOrderBook(order);
        updateLimitBook(order);

        return null;
    }

    private void updateLimitBook(Order order) {
        KeyValueStore<String, LimitBookEntry> store = null;
        if (order.getType().toString().equals("BUY")) {
            store = limitBookBuy;
        } else if (order.getType().toString().equals("SELL")) {
            store = limitBookSell;
        }

        boolean found = false;
        char[] toRange = order.getInstrument().toString().toCharArray();
        toRange[toRange.length - 1] = (char) (toRange[toRange.length - 1] + 1);
        try (var iterator = store.range(order.getInstrument().toString(), toRange.toString())) {
            while (iterator.hasNext()) {
                KeyValue<String, LimitBookEntry> next = iterator.next();
                LimitBookEntry limitBookEntry = next.value;
                if (limitBookEntry.getInstrument().equals(order.getInstrument())
                        && limitBookEntry.getPrice() == order.getPrice()
                ) {
                    limitBookEntry.setOrderCount(limitBookEntry.getOrderCount() + 1);
                    limitBookEntry.setQuantity(limitBookEntry.getQuantity() + order.getQuantity());
                    store.put(next.key, limitBookEntry);
                    found = true;
                    break;
                }
            }
        }
        if (! found) {
            LimitBookEntry limitBookEntry = LimitBookEntry.newBuilder()
                    .setOrderCount(1)
                    .setQuantity(order.getQuantity())
                    .setInstrument(order.getInstrument())
                    .setType(order.getType())
                    .setPrice(order.getPrice()).build();

            String compoundKey = order.getInstrument() + String.valueOf(order.getPrice());
            store.put(compoundKey, limitBookEntry);
        }
    }

    private void updateOrderBook(Order order) {
        String compoundKey = order.getInstrument() + String.valueOf(order.getId());
        if (order.getType().toString().equals("BUY")) {
            orderBookBuy.put(compoundKey, order);
        } else if (order.getType().toString().equals("SELL")) {
            orderBookSell.put(compoundKey, order);
        }
    }

    @Override
    public void close() {

    }
}
