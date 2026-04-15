package com.example.kafkaservice.intake;

import java.time.OffsetDateTime;

public record StagingInboxRecord(
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        String kafkaKey,
        String loadingId,
        String entityType,
        String rawMessage,
        ParseStatus parseStatus,
        IntakeStatus intakeStatus,
        String errorMessage,
        OffsetDateTime receivedAt,
        OffsetDateTime stagedAt
) {
}
