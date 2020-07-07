package io.confluent.dabz;

import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args) {
        Thread producerThread = new Thread(() -> {
            try {
                Producer.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        producerThread.start();

        Thread consumerThread = new Thread(() -> Consumer.main(args));
        consumerThread.start();

        SimpleWordCount.main(args);
    }
}
