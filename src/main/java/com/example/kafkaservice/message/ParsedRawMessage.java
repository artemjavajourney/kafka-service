package com.example.kafkaservice.message;

import com.example.kafkaservice.intake.ParseStatus;
import com.fasterxml.jackson.databind.JsonNode;

public record ParsedRawMessage(
        String loadingId,
        String entityType,
        JsonNode body,
        ParseStatus parseStatus,
        String errorMessage
) {
}
