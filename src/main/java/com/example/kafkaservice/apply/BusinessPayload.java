package com.example.kafkaservice.apply;

public record BusinessPayload(
        String entityType,
        String businessId,
        String parentBusinessId,
        String rawBody
) {
}
