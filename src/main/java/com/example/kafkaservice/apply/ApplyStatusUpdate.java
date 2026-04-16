package com.example.kafkaservice.apply;

import com.example.kafkaservice.audit.ProcessingLogStatus;

public record ApplyStatusUpdate(
        long stagingId,
        ProcessingLogStatus status,
        String errorMessage
) {
}
