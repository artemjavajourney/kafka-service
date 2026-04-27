package com.example.kafkaservice.intake;

import com.fasterxml.jackson.databind.JsonNode;

public record ParsedMessage(
        String loadingId,
        String entityType,
        JsonNode body,
        ParseStatus parseStatus,
        String errorMessage
) {
    public boolean isBodyUsable() {
        return body != null && body.isObject() && entityType != null && !entityType.isBlank();
    }
}
