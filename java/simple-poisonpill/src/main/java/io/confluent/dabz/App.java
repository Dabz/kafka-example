package io.confluent.dabz;

public class App {
    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(new SimpleConsumer());
        thread.start();
        SimpleProducer producer = new SimpleProducer();
        producer.run();
    }
}
