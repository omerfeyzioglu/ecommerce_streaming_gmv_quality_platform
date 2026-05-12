package com.example.flinkkafkasandbox;

import org.apache.flink.util.OutputTag;

public final class StreamTags {
    public static final OutputTag<String> INVALID_ORDERS = new OutputTag<>("invalid-orders") {
    };

    private StreamTags() {
    }
}
