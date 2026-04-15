package com.example.kafkaservice.audit;

import java.time.OffsetDateTime;

public record EventProcessingLogRecord(
        long stagingId,
        String loadingId,
        String entityType,
        ProcessingLogStatus status,
        int recordsReceived,
        int recordsInserted,
        int recordsUpdated,
        int recordsSkipped,
        String errorMessage,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
    public static EventProcessingLogRecord staged(long stagingId, String loadingId, String entityType, OffsetDateTime now) {
        return new EventProcessingLogRecord(
                stagingId,
                loadingId,
                entityType,
                ProcessingLogStatus.STAGED,
                1,
                0,
                0,
                0,
                null,
                now,
                now
        );
    }
}
