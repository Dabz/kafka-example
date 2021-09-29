package io.confluent.dabz;

import io.confluent.dabz.model.LineAuthor;
import io.confluent.dabz.model.ShakespeareKey;
import io.confluent.dabz.model.ShakespeareValue;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AvroProducer {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.setProperty(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());

        KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(properties);
        HashMap<String, Integer> years = new HashMap<String, Integer>();
        years.put("Hamlet", 1600);
        years.put("Julius Caesar", 1599);
        years.put("Macbeth", 1605);
        years.put("Merchant of Venice", 1596);
        years.put("Othello", 1604);
        years.put("Romeo and Juliet", 1594);

        File directory = new File(producer.getClass().getClassLoader().getResource("shakespeare").getFile());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close(1, TimeUnit.MINUTES)));

        for (File file : directory.listFiles()) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String key = file.getName().split("\\.")[0];
            String line;
            while ((line = reader.readLine()) != null) {
                String lineNumberString = line.substring(0, 8).trim();
                String text = line.substring(8).trim();

                ShakespeareValue shakespeareValue = new ShakespeareValue();
                shakespeareValue.setLine(text);
                shakespeareValue.setLineNumber(Integer.valueOf(lineNumberString));
                shakespeareValue.setProut(10);
                shakespeareValue.setProut2(10);
                shakespeareValue.setAuthor(new LineAuthor("William", "Shakespear"));

                ShakespeareKey shakespeareKey = new ShakespeareKey();
                shakespeareKey.setYear(years.get(key));
                shakespeareKey.setWork(key);

                producer.send(new ProducerRecord<Object, Object>("fflowpilotdataplatform", shakespeareKey, shakespeareValue),
                        (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            }
                        });
            }
        }
    }
}
