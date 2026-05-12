package com.example.flinkkafkasandbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

public final class OrderJson {
    private static final ObjectMapper JSON = new ObjectMapper();

    private OrderJson() {
    }

    public static JsonNode parse(String rawJson) throws Exception {
        return JSON.readTree(rawJson);
    }

    public static String addProcessedAt(String rawJson) throws Exception {
        ObjectNode order = (ObjectNode) JSON.readTree(rawJson);
        order.put("processedAt", Instant.now().toString());
        return JSON.writeValueAsString(order);
    }

    public static String addLearningStep(String rawJson) throws Exception {
        ObjectNode order = (ObjectNode) JSON.readTree(rawJson);
        order.put("learningStep", "map-course");
        return JSON.writeValueAsString(order);
    }

    public static String invalidOrder(String rawPayload, String reason) throws Exception {
        ObjectNode invalidOrder = JSON.createObjectNode();
        invalidOrder.put("reason", reason);
        invalidOrder.put("rawPayload", rawPayload);
        invalidOrder.put("failedAt", Instant.now().toString());
        return JSON.writeValueAsString(invalidOrder);
    }

    public static String gmvMetric(long windowStart, long windowEnd, String currency, GmvAccumulator accumulator)
            throws Exception {
        ObjectNode metric = JSON.createObjectNode();
        metric.put("windowStart", Instant.ofEpochMilli(windowStart).toString());
        metric.put("windowEnd", Instant.ofEpochMilli(windowEnd).toString());
        metric.put("currency", currency);
        metric.put("totalGmv", Math.round(accumulator.totalGmv() * 100.0) / 100.0);
        metric.put("orderCount", accumulator.orderCount());
        metric.put("updatedAt", Instant.now().toString());
        return JSON.writeValueAsString(metric);
    }

    public static String eventId(String rawJson) throws Exception {
        return JSON.readTree(rawJson).get("eventId").asText();
    }

    public static String currency(String rawJson) throws Exception {
        return JSON.readTree(rawJson).get("currency").asText();
    }

    public static boolean isPaymentCompleted(String rawJson) throws Exception {
        JsonNode order = JSON.readTree(rawJson);
        return "payment-completed".equals(order.get("eventType").asText());
    }

    public static long eventTimeMillisOrFallback(String rawJson, long fallbackTimestamp) {
        try {
            JsonNode order = JSON.readTree(rawJson);
            return Instant.parse(order.get("eventTime").asText()).toEpochMilli();
        } catch (Exception e) {
            return fallbackTimestamp > 0 ? fallbackTimestamp : System.currentTimeMillis();
        }
    }

    public static boolean isMissingText(JsonNode node, String fieldName) {
        return !node.has(fieldName) || node.get(fieldName).isNull() || node.get(fieldName).asText().isBlank();
    }
}
