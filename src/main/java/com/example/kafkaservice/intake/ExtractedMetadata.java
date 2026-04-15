package com.example.kafkaservice.intake;

public record ExtractedMetadata(
        String loadingId,
        String entityType,
        ParseStatus parseStatus,
        String errorMessage
) {
}
