package com.example.kafkaservice.intake;

import java.time.OffsetDateTime;

public record StagingInboxRecord(
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        String kafkaKey,
        String rawMessage,
        OffsetDateTime receivedAt
) {
}
