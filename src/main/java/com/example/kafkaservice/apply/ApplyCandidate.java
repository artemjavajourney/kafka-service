package com.example.kafkaservice.apply;

public record ApplyCandidate(
        long stagingId,
        String loadingId,
        String entityType,
        String rawMessage
) {
}
