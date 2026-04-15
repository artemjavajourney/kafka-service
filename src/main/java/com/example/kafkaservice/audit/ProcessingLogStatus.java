package com.example.kafkaservice.audit;

public enum ProcessingLogStatus {
    STAGED,
    APPLIED,
    SKIPPED,
    FAILED,
    DEFERRED
}
