package io.confluent.dabz.serde;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;

public class SerdeFactory {
    public static <T extends SpecificRecord> Serde<T> getSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        HashMap<String, String> map = new HashMap<>();
        map.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        serde.configure(map, false);
        return serde;
    }

}
