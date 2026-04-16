package com.example.kafkaservice.audit;

public enum ProcessingLogStatus {
    STAGED,
    PROCESSING,
    APPLIED,
    SKIPPED,
    FAILED,
    DEFERRED
}
