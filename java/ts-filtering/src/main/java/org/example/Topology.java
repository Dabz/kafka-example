package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;
import org.example.model.Customer;
import org.example.model.CustomerEnvelope;
import org.example.transformer.TimeBasedFilter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.example.Constant.*;

public class Topology {


    public static void addTopology(StreamsBuilder streamsBuilder) {
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(TS_STORE),
                Serdes.String(),
                Serdes.Long()
        ));

        streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Context.getContext().<CustomerEnvelope>getJsonSerde(CustomerEnvelope.class)))
                .flatMap((key, value) -> explodeMerge(key, value))
                .repartition(Repartitioned.with(Serdes.String(), Context.getContext().<CustomerEnvelope>getJsonSerde(CustomerEnvelope.class)).withName(REPARTITION_BY_KEY))
                .transform(() -> new TimeBasedFilter(), TS_STORE)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Context.getContext().getJsonSerde(CustomerEnvelope.class)));
    }

    private static Collection<KeyValue<String, CustomerEnvelope>> explodeMerge(String key, CustomerEnvelope value) {
        if (MERGE_OP.equals(value.getOp())) {
            Customer customerToDelete = value.getCustomerBefore();
            Customer customerToUpdate = value.getCustomerAfter();

            CustomerEnvelope customerEnvelopeDelete = value.copy();
            customerEnvelopeDelete.setCustomerBefore(customerToDelete);
            customerEnvelopeDelete.setCustomerAfter(null);
            customerEnvelopeDelete.setOp(DELETE_OP);

            CustomerEnvelope customerEnvelopeUpdate = value.copy();
            customerEnvelopeUpdate.setCustomerAfter(customerToUpdate);
            customerEnvelopeUpdate.setCustomerBefore(customerToUpdate);
            customerEnvelopeUpdate.setOp(UPDATE_OP);
            return Arrays.asList(
                    new KeyValue<>(customerToDelete.getId(), customerEnvelopeDelete),
                    new KeyValue<>(customerToUpdate.getId(), customerEnvelopeUpdate));
        }
        return Collections.singleton(new KeyValue<>(key, value));
    }
}
