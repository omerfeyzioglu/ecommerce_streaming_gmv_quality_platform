package com.example.flinkkafkasandbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.OutputTag;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Set;

public class OrderStreamingJob {
    private static final String BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String INPUT_TOPIC = "orders";
    private static final String OUTPUT_TOPIC = "enriched-orders";
    private static final String DLQ_TOPIC = "invalid-orders";
    private static final String CONSUMER_GROUP_ID = "flink-kafka-sandbox";
    private static final Set<String> VALID_EVENT_TYPES = Set.of(
            "order-created",
            "payment-completed",
            "order-cancelled",
            "payment-failed"
    );

    private static final OutputTag<String> INVALID_ORDER_TAG = new OutputTag<>("invalid-orders") {
    };

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Lesson 1: Flink job anatomy.
        // env, source, DataStream transformation, and sink are the core pieces to learn first.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Source: Flink reads raw order JSON strings from Kafka.
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> orders = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Read orders from Kafka"
        );

        SingleOutputStreamOperator<String> validOrders = orders
                .process(new RouteInvalidOrders())
                .name("Route invalid orders to DLQ");

        validOrders
                .map(OrderStreamingJob::addProcessedAt)
                .name("Add processedAt field")
                .map(OrderStreamingJob::addLearningStepForLessonOne)
                .name("map step L1")
                .sinkTo(kafkaSink(OUTPUT_TOPIC))
                .name("Write enriched orders to Kafka");

        validOrders
                .getSideOutput(INVALID_ORDER_TAG)
                .sinkTo(kafkaSink(DLQ_TOPIC))
                .name("Write invalid orders to DLQ");

        env.execute("Simple Kafka to Flink Job");
    }

    private static String addProcessedAt(String rawJson) throws Exception {
        ObjectNode order = (ObjectNode) JSON.readTree(rawJson);
        order.put("processedAt", Instant.now().toString());
        return JSON.writeValueAsString(order);
    }

    private static String addLearningStepForLessonOne(String rawJson) throws Exception {
        ObjectNode order = (ObjectNode) JSON.readTree(rawJson);
        order.put("learningStep" , "map-course");
        return JSON.writeValueAsString(order);
    }

    private static KafkaSink<String> kafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }

    private static final class RouteInvalidOrders extends ProcessFunction<String, String> {
        @Override
        public void processElement(String rawJson, Context context, Collector<String> validOrders) throws Exception {
            ValidationResult validation = validateOrder(rawJson);

            if (validation.isValid()) {
                validOrders.collect(rawJson);
                return;
            }

            context.output(INVALID_ORDER_TAG, invalidOrder(rawJson, validation.reason()));
        }
    }

    private static ValidationResult validateOrder(String rawJson) {
        JsonNode order;
        try {
            order = JSON.readTree(rawJson);
        } catch (Exception e) {
            return ValidationResult.invalid("malformed-json");
        }

        if (!order.isObject()) {
            return ValidationResult.invalid("json-is-not-object");
        }
        if (isMissingText(order, "orderId")) {
            return ValidationResult.invalid("missing-orderId");
        }
        if (isMissingText(order, "userId")) {
            return ValidationResult.invalid("missing-userId");
        }
        if (isMissingText(order, "currency")) {
            return ValidationResult.invalid("missing-currency");
        }
        if (isMissingText(order, "eventTime")) {
            return ValidationResult.invalid("missing-eventTime");
        }
        if (!order.has("amount") || !order.get("amount").isNumber()) {
            return ValidationResult.invalid("invalid-amount");
        }
        if (order.get("amount").asDouble() <= 0) {
            return ValidationResult.invalid("non-positive-amount");
        }
        if (isMissingText(order, "eventType")) {
            return ValidationResult.invalid("missing-eventType");
        }
        if (!VALID_EVENT_TYPES.contains(order.get("eventType").asText())) {
            return ValidationResult.invalid("unknown-eventType");
        }

        return ValidationResult.valid();
    }

    private static boolean isMissingText(JsonNode node, String fieldName) {
        return !node.has(fieldName) || node.get(fieldName).isNull() || node.get(fieldName).asText().isBlank();
    }

    private static String invalidOrder(String rawPayload, String reason) throws Exception {
        ObjectNode invalidOrder = JSON.createObjectNode();
        invalidOrder.put("reason", reason);
        invalidOrder.put("rawPayload", rawPayload);
        invalidOrder.put("failedAt", Instant.now().toString());
        return JSON.writeValueAsString(invalidOrder);
    }

    private record ValidationResult(boolean isValid, String reason) {
        private static ValidationResult valid() {
            return new ValidationResult(true, "");
        }

        private static ValidationResult invalid(String reason) {
            return new ValidationResult(false, reason);
        }
    }
}
