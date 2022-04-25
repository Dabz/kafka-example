package io.confluent.dabz;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Properties;

public class SerdeFactory {
    public static <T extends SpecificRecord> Serde<T> createSerde() {
        HashMap<String, String> config = new HashMap<>();
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");

        SpecificAvroSerde<T> serde = new SpecificAvroSerde<T>();
        serde.configure(config, false);

        return serde;
    }
}
