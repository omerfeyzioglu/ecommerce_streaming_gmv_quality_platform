package com.example.flinkkafkasandbox;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class AddGmvWindowMetadata extends ProcessWindowFunction<GmvAccumulator, String, String, TimeWindow> {
    @Override
    public void process(String currency, Context context, Iterable<GmvAccumulator> values, Collector<String> out)
            throws Exception {
        GmvAccumulator accumulator = values.iterator().next();
        out.collect(OrderJson.gmvMetric(
                context.window().getStart(),
                context.window().getEnd(),
                currency,
                accumulator
        ));
    }
}
