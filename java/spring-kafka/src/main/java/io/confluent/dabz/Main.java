package io.confluent.dabz;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.backoff.FixedBackOff;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Main implements CommandLineRunner {
    public static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args).close();
    }

    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    ExecutorService executor = Executors.newFixedThreadPool(10);

    private final CountDownLatch latch = new CountDownLatch(4);

    @Override
    public void run(String... args) throws Exception {
        this.template.send("myTopic", "foo1");
        this.template.send("myTopic", "foo2");
        this.template.send("myTopic", "foo3");
        latch.await(60, TimeUnit.SECONDS);
        logger.info("All received");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactor
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(kafkaConsumerFactor);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(1000L, 2L)));
        return factory;
    }

    @KafkaListener(topics = "myTopic", id = "orch")
    public void listen(ConsumerRecord<?, ?> cr, Acknowledgment acknowledgment, Consumer<?, ?> consumer) throws Exception {
        logger.info(cr.toString());
        registry.getListenerContainer("orch").pause();
        Runnable runnable = () -> {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            acknowledgment.acknowledge();
            registry.getListenerContainer("orch").resume();
            latch.countDown();
        };
        executor.submit(runnable);
    }
}
