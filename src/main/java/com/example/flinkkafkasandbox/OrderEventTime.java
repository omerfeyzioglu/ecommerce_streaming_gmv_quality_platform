package com.example.flinkkafkasandbox;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public final class OrderEventTime {
    private OrderEventTime() {
    }

    public static WatermarkStrategy<String> watermarkStrategy() {
        return WatermarkStrategy
                .<String>forBoundedOutOfOrderness(JobConfig.MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner(new TimestampAssigner());
    }

    private static final class TimestampAssigner implements SerializableTimestampAssigner<String> {
        @Override
        public long extractTimestamp(String rawJson, long recordTimestamp) {
            return OrderJson.eventTimeMillisOrFallback(rawJson, recordTimestamp);
        }
    }
}
