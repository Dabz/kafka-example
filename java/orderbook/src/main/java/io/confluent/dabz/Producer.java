package io.confluent.dabz;

import io.confluent.bootcamp.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

public class Producer implements Runnable {
    @Override
    public void run() {
        try {
            doRun();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doRun() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<String, Order> kafkaProducer = new KafkaProducer<>(properties);
        AtomicReference<Boolean> isRunning = new AtomicReference<>(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isRunning.set(false)));
        var instruments = new String[]{"TRUMP", "SHB", "BTC"};

        while (isRunning.get()) {
            var instrument = instruments[ThreadLocalRandom.current().nextInt(0, 3)];
            Order build = Order.newBuilder()
                    .setType(ThreadLocalRandom.current().nextInt(0, 2) >= 1 ? "BUY" : "SELL")
                    .setQuantity(ThreadLocalRandom.current().nextInt(0, 10))
                    .setPrice(ThreadLocalRandom.current().nextInt(0, 5))
                    .setId(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE)
                    .setInstrument(instrument)
                    .build();

            try {
                kafkaProducer.send(new ProducerRecord<>(Constant.Order, instrument, build)).get();
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
