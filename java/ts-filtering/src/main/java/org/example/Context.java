package org.example;

import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;

import java.util.HashMap;
import java.util.Properties;

public class Context {
    private static Context context;
    private HashMap<String, String> properties;

    public void registerProperties(Properties lp) {
        properties = new HashMap<>();
        lp.forEach((key, value) -> properties.put(key.toString(), value.toString()));
    }

    public static Context getContext() {
        if (context == null) {
            context = new Context();
        }
        return context;
    }

    public HashMap<String, String> getSerdeProperties() {
        return properties;
    }

    public <T>KafkaJsonSchemaSerde<T> getJsonSerde(Class<T> cls) {
        var serde = new KafkaJsonSchemaSerde<T>(cls);
        serde.configure(properties, false);
        return serde;
    }
}
