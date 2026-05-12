package com.example.flinkkafkasandbox;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class DeduplicateByEventId extends KeyedProcessFunction<String, String, String> {
    private ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(JobConfig.DEDUP_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(String rawJson, Context context, Collector<String> uniqueOrders) throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            context.output(StreamTags.INVALID_ORDERS, OrderJson.invalidOrder(rawJson, "duplicate-event"));
            return;
        }

        seen.update(true);
        uniqueOrders.collect(rawJson);
    }
}
