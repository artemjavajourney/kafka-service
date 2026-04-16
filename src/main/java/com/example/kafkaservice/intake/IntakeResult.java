package com.example.kafkaservice.intake;

public record IntakeResult(
        long stagingId,
        String loadingId,
        String entityType,
        ParseStatus parseStatus
) {
}
