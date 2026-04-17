package com.example.kafkaservice.apply;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class BusinessPayloadExtractor {

    private final ObjectMapper objectMapper;

    public BusinessPayload extract(String rawMessage, String fallbackEntityType) {
        try {
            JsonNode root = objectMapper.readTree(rawMessage);
            JsonNode resultJson = root.path("result_json");
            JsonNode inner = resultJson;

            if (resultJson.isTextual()) {
                inner = objectMapper.readTree(resultJson.asText());
            }

            JsonNode body = inner.path("body");
            String entityType = firstText(body, "entity_type", "entityType", "event_type_id", "eventTypeId");
            if (entityType == null || entityType.isBlank()) {
                entityType = fallbackEntityType;
            }

            return new BusinessPayload(entityType, body);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse raw message for apply: " + e.getMessage(), e);
        }
    }

    public String firstText(JsonNode node, String... fieldNames) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }

        for (String fieldName : fieldNames) {
            JsonNode value = node.get(fieldName);
            if (value != null && !value.isNull()) {
                String text = value.asText(null);
                if (text != null && !text.isBlank()) {
                    return text;
                }
            }
        }

        return null;
    }

    public Integer firstInt(JsonNode node, String... fieldNames) {
        String text = firstText(node, fieldNames);
        if (text == null) {
            return null;
        }

        try {
            return Integer.valueOf(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Boolean firstBoolean(JsonNode node, String... fieldNames) {
        String text = firstText(node, fieldNames);
        if (text == null) {
            return null;
        }

        if ("true".equalsIgnoreCase(text) || "1".equals(text)) {
            return true;
        }
        if ("false".equalsIgnoreCase(text) || "0".equals(text)) {
            return false;
        }
        return null;
    }
}
