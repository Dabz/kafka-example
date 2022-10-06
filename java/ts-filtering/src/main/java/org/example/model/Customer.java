package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Customer {
    @JsonProperty
    private String id;
    @JsonProperty
    private String firstName;
    @JsonProperty
    private String lastName;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
