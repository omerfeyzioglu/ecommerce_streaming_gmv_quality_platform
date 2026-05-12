package com.example.flinkkafkasandbox;

public record ValidationResult(boolean isValid, String reason) {
    public static ValidationResult valid() {
        return new ValidationResult(true, "");
    }

    public static ValidationResult invalid(String reason) {
        return new ValidationResult(false, reason);
    }
}
