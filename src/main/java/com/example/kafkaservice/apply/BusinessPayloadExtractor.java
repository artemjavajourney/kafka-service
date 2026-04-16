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

            String businessId = firstText(body, "id", "entity_id", "entityId", "business_id", "businessId");
            String parentBusinessId = firstText(body, "entity1_id", "parent_id", "parentId", "parent_entity_id", "parentEntityId");

            return new BusinessPayload(
                    entityType,
                    businessId,
                    parentBusinessId,
                    body.toString()
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse raw message for apply: " + e.getMessage(), e);
        }
    }

    private String firstText(JsonNode node, String... fieldNames) {
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
}
