package com.example.flinkkafkasandbox;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OrderStreamingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint lesson: Flink periodically snapshots state + Kafka progress for recovery.
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        DataStream<String> orders = env.fromSource(
                KafkaTopics.ordersSource(),
                OrderEventTime.watermarkStrategy(),
                "Read orders from Kafka"
        );

        SingleOutputStreamOperator<String> validOrders = orders
                .process(new InvalidOrderRouter())
                .name("Route invalid orders to DLQ");

        SingleOutputStreamOperator<String> uniqueOrders = validOrders
                .keyBy(OrderJson::eventId)
                .process(new DeduplicateByEventId())
                .name("Deduplicate by eventId");

        uniqueOrders
                .map(OrderJson::addProcessedAt)
                .name("Add processedAt field")
                .map(OrderJson::addLearningStep)
                .name("Add learning step")
                .sinkTo(KafkaTopics.stringSink(JobConfig.ENRICHED_ORDERS_TOPIC))
                .name("Write enriched orders to Kafka");

        uniqueOrders
                .filter(OrderJson::isPaymentCompleted)
                .name("Keep payment-completed orders")
                .keyBy(OrderJson::currency)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new GmvAggregate(), new AddGmvWindowMetadata())
                .name("Compute 1-minute GMV by currency")
                .sinkTo(KafkaTopics.stringSink(JobConfig.GMV_BY_MINUTE_TOPIC))
                .name("Write GMV windows to Kafka");

        DataStream<String> invalidOrders = validOrders
                .getSideOutput(StreamTags.INVALID_ORDERS)
                .union(uniqueOrders.getSideOutput(StreamTags.INVALID_ORDERS));

        invalidOrders
                .sinkTo(KafkaTopics.stringSink(JobConfig.INVALID_ORDERS_TOPIC))
                .name("Write invalid orders to DLQ");

        env.execute("Kafka Orders Learning Job");
    }
}
