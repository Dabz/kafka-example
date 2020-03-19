package io.confluent.dabz;

import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException {
        AvroProducer producer = new AvroProducer();
        new Thread(new AvroConsumer()).start();
        producer.main(null);
    }
}
