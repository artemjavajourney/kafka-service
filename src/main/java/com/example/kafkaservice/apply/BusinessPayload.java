package com.example.kafkaservice.apply;

import com.fasterxml.jackson.databind.JsonNode;

public record BusinessPayload(
        String entityType,
        JsonNode body
) {
}
