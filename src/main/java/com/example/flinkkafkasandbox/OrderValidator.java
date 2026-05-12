package com.example.flinkkafkasandbox;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Set;

public final class OrderValidator {
    private static final Set<String> VALID_EVENT_TYPES = Set.of(
            "order-created",
            "payment-completed",
            "order-cancelled",
            "payment-failed"
    );

    private OrderValidator() {
    }

    public static ValidationResult validate(String rawJson) {
        JsonNode order;
        try {
            order = OrderJson.parse(rawJson);
        } catch (Exception e) {
            return ValidationResult.invalid("malformed-json");
        }

        if (!order.isObject()) {
            return ValidationResult.invalid("json-is-not-object");
        }
        if (OrderJson.isMissingText(order, "eventId")) {
            return ValidationResult.invalid("missing-eventId");
        }
        if (OrderJson.isMissingText(order, "orderId")) {
            return ValidationResult.invalid("missing-orderId");
        }
        if (OrderJson.isMissingText(order, "userId")) {
            return ValidationResult.invalid("missing-userId");
        }
        if (OrderJson.isMissingText(order, "currency")) {
            return ValidationResult.invalid("missing-currency");
        }
        if (OrderJson.isMissingText(order, "eventTime")) {
            return ValidationResult.invalid("missing-eventTime");
        }
        if (!order.has("amount") || !order.get("amount").isNumber()) {
            return ValidationResult.invalid("invalid-amount");
        }
        if (order.get("amount").asDouble() <= 0) {
            return ValidationResult.invalid("non-positive-amount");
        }
        if (OrderJson.isMissingText(order, "eventType")) {
            return ValidationResult.invalid("missing-eventType");
        }
        if (!VALID_EVENT_TYPES.contains(order.get("eventType").asText())) {
            return ValidationResult.invalid("unknown-eventType");
        }

        return ValidationResult.valid();
    }
}
