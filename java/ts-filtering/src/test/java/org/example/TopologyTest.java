package org.example;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.example.model.Customer;
import org.example.model.CustomerEnvelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.example.Constant.*;

class TopologyTest {

    private TestInputTopic<String, CustomerEnvelope> inputTopic;
    private TestOutputTopic<String, CustomerEnvelope> outputTopic;
    private TopologyTestDriver testDriver;

    @BeforeEach
    void setUp() {
        initializeContext();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Topology.addTopology(streamsBuilder);

        testDriver = new TopologyTestDriver(streamsBuilder.build());
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new StringSerializer(), Context.getContext().<CustomerEnvelope>getJsonSerde(CustomerEnvelope.class).serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), Context.getContext().<CustomerEnvelope>getJsonSerde(CustomerEnvelope.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    private static void initializeContext() {
        Properties contextProperties = new Properties();
        contextProperties.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://local");
        Context.getContext().registerProperties(contextProperties);
    }

    @Test
    void noFilterHappening() {
        var user = new Customer();
        user.setFirstName("Damien");
        user.setLastName("Gasparina");
        user.setId("111");

        CustomerEnvelope customerCreation = new CustomerEnvelope();
        customerCreation.setOp(INSERT_OP);
        customerCreation.setTs(Instant.ofEpochMilli(111L));
        customerCreation.setCustomerBefore(null);
        customerCreation.setCustomerAfter(user);

        CustomerEnvelope customerUpdate = new CustomerEnvelope();
        customerUpdate.setOp(UPDATE_OP);
        customerUpdate.setTs(Instant.ofEpochMilli(112L));
        customerUpdate.setCustomerBefore(null);
        customerUpdate.setCustomerAfter(user);

        CustomerEnvelope customerDelete = new CustomerEnvelope();
        customerDelete.setOp(DELETE_OP);
        customerDelete.setTs(Instant.ofEpochMilli(113L));
        customerDelete.setCustomerBefore(null);
        customerDelete.setCustomerAfter(user);

        inputTopic.pipeInput("111", customerCreation);
        inputTopic.pipeInput("111", customerUpdate);
        inputTopic.pipeInput("111", customerDelete);

        var keyValues = outputTopic.readKeyValuesToList();
        Assertions.assertEquals(3, keyValues.size());
        Assertions.assertEquals(DELETE_OP, keyValues.get(2).value.getOp());
    }

    @Test
    void filterLastCreate() {
        var user = new Customer();
        user.setFirstName("Damien");
        user.setLastName("Gasparina");
        user.setId("111");

        CustomerEnvelope customerCreation = new CustomerEnvelope();
        customerCreation.setOp(INSERT_OP);
        customerCreation.setTs(Instant.ofEpochMilli(111L));
        customerCreation.setCustomerBefore(null);
        customerCreation.setCustomerAfter(user);

        CustomerEnvelope customerDelete = new CustomerEnvelope();
        customerDelete.setOp(DELETE_OP);
        customerDelete.setTs(Instant.ofEpochMilli(113L));
        customerDelete.setCustomerBefore(null);
        customerDelete.setCustomerAfter(user);

        CustomerEnvelope customerUpdate = new CustomerEnvelope();
        customerUpdate.setOp(UPDATE_OP);
        customerUpdate.setTs(Instant.ofEpochMilli(112L));
        customerUpdate.setCustomerBefore(null);
        customerUpdate.setCustomerAfter(user);

        inputTopic.pipeInput("111", customerCreation);
        inputTopic.pipeInput("111", customerDelete);
        inputTopic.pipeInput("111", customerUpdate);

        var keyValues = outputTopic.readKeyValuesToList();
        Assertions.assertEquals(2, keyValues.size());
        Assertions.assertEquals(DELETE_OP, keyValues.get(1).value.getOp());
    }
}