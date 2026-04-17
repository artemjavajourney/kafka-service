package com.example.kafkaservice.apply;

import com.example.kafkaservice.audit.ProcessingLogStatus;

public record ApplyStatusUpdate(
        long stagingId,
        ProcessingLogStatus status,
        String errorMessage,
        int recordsInserted,
        int recordsUpdated,
        int recordsSkipped
) {
    public static ApplyStatusUpdate inserted(long stagingId) {
        return new ApplyStatusUpdate(stagingId, ProcessingLogStatus.APPLIED, null, 1, 0, 0);
    }

    public static ApplyStatusUpdate updated(long stagingId) {
        return new ApplyStatusUpdate(stagingId, ProcessingLogStatus.APPLIED, null, 0, 1, 0);
    }

    public static ApplyStatusUpdate skipped(long stagingId, String message) {
        return new ApplyStatusUpdate(stagingId, ProcessingLogStatus.SKIPPED, message, 0, 0, 1);
    }

    public static ApplyStatusUpdate deferred(long stagingId, String message) {
        return new ApplyStatusUpdate(stagingId, ProcessingLogStatus.DEFERRED, message, 0, 0, 0);
    }

    public static ApplyStatusUpdate failed(long stagingId, String message) {
        return new ApplyStatusUpdate(stagingId, ProcessingLogStatus.FAILED, message, 0, 0, 0);
    }
}
