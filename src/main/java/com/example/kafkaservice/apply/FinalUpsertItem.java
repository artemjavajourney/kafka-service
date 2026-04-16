package com.example.kafkaservice.apply;

public record FinalUpsertItem(
        long stagingId,
        String businessId,
        String parentBusinessId,
        String payload
) {
}
