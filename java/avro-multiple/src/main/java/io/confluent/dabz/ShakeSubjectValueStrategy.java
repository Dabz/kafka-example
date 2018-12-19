package io.confluent.dabz;

import io.confluent.dabz.model.ShakespeareKey;
import io.confluent.dabz.model.ShakespeareValue;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.avro.Schema;

import java.util.Map;

public class ShakeSubjectValueStrategy implements SubjectNameStrategy {
    public String subjectName(String s, boolean b, Object o) {
        if (o instanceof Schema) {
            Schema sr = (Schema) o;
            if (sr.getName().equals("ShakespeareValue")) {
                return "bougavalue2";
            }
            if (sr.getName().equals("ShakespeareKey")) {
                return "bougakey2";
            }
        }
        return "bouga-what-the-fuck";
    }

    public void configure(Map<String, ?> map) {

    }
}
