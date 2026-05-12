package com.example.flinkkafkasandbox;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public final class InvalidOrderRouter extends ProcessFunction<String, String> {
    @Override
    public void processElement(String rawJson, Context context, Collector<String> validOrders) throws Exception {
        ValidationResult validation = OrderValidator.validate(rawJson);

        if (validation.isValid()) {
            validOrders.collect(rawJson);
            return;
        }

        context.output(StreamTags.INVALID_ORDERS, OrderJson.invalidOrder(rawJson, validation.reason()));
    }
}
