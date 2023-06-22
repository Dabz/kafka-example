package io.confluent.dabz;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

public class SerdeGenerator {
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Context.getConfiguration(), false);
        return serde;
    }
}
