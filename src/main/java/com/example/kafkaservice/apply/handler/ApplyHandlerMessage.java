package com.example.kafkaservice.apply.handler;

import com.fasterxml.jackson.databind.JsonNode;

public record ApplyHandlerMessage(
        long stagingId,
        JsonNode body
) {
}
