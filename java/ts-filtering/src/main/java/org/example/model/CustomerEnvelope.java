package org.example.model;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class CustomerEnvelope {
    @JsonProperty
    private String op;

    @JsonProperty
    private Instant ts;

    @JsonProperty
    private Customer customerAfter;

    @JsonProperty
    private Customer customerBefore;

    public CustomerEnvelope copy() {
        CustomerEnvelope envelope = new CustomerEnvelope();
        envelope.setCustomerAfter(this.getCustomerAfter());
        envelope.setCustomerBefore(this.getCustomerBefore());
        envelope.setTs(this.getTs());
        envelope.setOp(this.getOp());
        return envelope;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Instant getTs() {
        return ts;
    }

    public void setTs(Instant ts) {
        this.ts = ts;
    }

    public Customer getCustomerAfter() {
        return customerAfter;
    }

    public void setCustomerAfter(Customer customerAfter) {
        this.customerAfter = customerAfter;
    }

    public Customer getCustomerBefore() {
        return customerBefore;
    }

    public void setCustomerBefore(Customer customerBefore) {
        this.customerBefore = customerBefore;
    }
}
