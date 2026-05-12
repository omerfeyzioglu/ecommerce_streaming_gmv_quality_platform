package com.example.flinkkafkasandbox;

import java.time.Duration;

public final class JobConfig {
    public static final String BOOTSTRAP_SERVERS = "kafka:29092";
    public static final String ORDERS_TOPIC = "orders";
    public static final String ENRICHED_ORDERS_TOPIC = "enriched-orders";
    public static final String INVALID_ORDERS_TOPIC = "invalid-orders";
    public static final String GMV_BY_MINUTE_TOPIC = "gmv-by-minute";
    public static final String CONSUMER_GROUP_ID = "flink-kafka-sandbox";

    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(10);
    public static final Duration DEDUP_TTL = Duration.ofHours(1);

    private JobConfig() {
    }
}
