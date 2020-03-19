package io.confluent.dabz;

public class Main {

    public static void main(String[] args) {
        for (int i = 0; i < 3; i++) {
            Thread dataProducer = new Thread(new Producer());
            dataProducer.start();
        }
        Thread stream = new Thread(new Stream());

        stream.start();
    }
}
