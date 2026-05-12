package com.example.flinkkafkasandbox;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class GmvAggregate implements AggregateFunction<String, GmvAccumulator, GmvAccumulator> {
    @Override
    public GmvAccumulator createAccumulator() {
        return new GmvAccumulator();
    }

    @Override
    public GmvAccumulator add(String rawJson, GmvAccumulator accumulator) {
        try {
            JsonNode order = OrderJson.parse(rawJson);
            accumulator.add(order.get("amount").asDouble());
        } catch (Exception ignored) {
            // The stream is validated before this aggregation.
        }
        return accumulator;
    }

    @Override
    public GmvAccumulator getResult(GmvAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public GmvAccumulator merge(GmvAccumulator left, GmvAccumulator right) {
        left.merge(right);
        return left;
    }
}
