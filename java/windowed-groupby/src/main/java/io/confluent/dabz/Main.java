package io.confluent.dabz;

public class Main {

    public static void main(String[] args) {
        Thread dataProducer = new Thread(new Producer());
        Thread stream = new Thread(new Stream());

        dataProducer.start();
        stream.start();
    }
}
