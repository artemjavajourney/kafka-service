package com.example.kafkaservice.apply;

import com.fasterxml.jackson.databind.JsonNode;

public record ApplyCandidate(
        long stagingId,
        String loadingId,
        String entityType,
        JsonNode body
) {
}
