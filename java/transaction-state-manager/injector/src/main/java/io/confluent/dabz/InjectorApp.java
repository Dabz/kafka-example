package io.confluent.dabz;

import io.confluent.dabz.model.Transaction;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.util.Properties;

public class InjectorApp {
    private KafkaProducer<String, Transaction> producer;
    private static final long NUMBER_OF_TRANSACTION_TO_SEND = 20 * 1000000;

    public static void main(String[] args) throws Exception {
        InjectorApp streamsApp = new InjectorApp();
        streamsApp.start(args);
    }

    private void start(String[] args) throws Exception {
        Properties properties = new Properties();
        if (args.length > 0) {
            properties.load(new FileInputStream(args[0]));
        } else {
            properties.load(this.getClass().getResourceAsStream("/kafka.properties"));
        }

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        this.producer = new KafkaProducer<String, io.confluent.dabz.model.Transaction>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

        for (long i = 0; i < NUMBER_OF_TRANSACTION_TO_SEND; i++) {
            var lei = randomDataGenerator.nextHexString(20);
            var txnRef = randomDataGenerator.nextHexString(30);

            Transaction transaction = Transaction.newBuilder()
                    .setTRANSACTIONID(randomDataGenerator.nextLong(0, 9999999999L))
                    .setREGULATORID(randomDataGenerator.nextHexString(5))
                    .build();

            this.producer.send(new ProducerRecord<>("transactions", lei + "#" + txnRef, transaction));
        }
    }
}