package org.example;

public class Constant {
    // TOPIC
    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "input-filtered";


    // STORE
    public static final String TS_STORE = "ts";

    // REPARTITION
    public static final String REPARTITION_BY_KEY = "after-merge";

    // ENUM
    public static final String MERGE_OP = "MERGE";
    public static final String INSERT_OP = "INSERT";
    public static final String UPDATE_OP = "UPDATE";
    public static final String DELETE_OP = "DELETE";
}
