package com.example.kafkaservice.intake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RawMessageParser {

    private final ObjectMapper objectMapper;

    public ParsedMessage parse(String rawMessage) {
        try {
            JsonNode root = objectMapper.readTree(rawMessage);
            String loadingId = firstText(root, "loading_id", "loadingId");

            JsonNode resultJsonNode = root.path("result_json");
            if (resultJsonNode.isMissingNode() || resultJsonNode.isNull()) {
                return failed(loadingId, ParseStatus.RAW_ONLY, "result_json is absent or null");
            }

            JsonNode inner;
            if (resultJsonNode.isTextual()) {
                try {
                    inner = objectMapper.readTree(resultJsonNode.asText());
                } catch (Exception e) {
                    return failed(loadingId, ParseStatus.RAW_ONLY, "Failed to parse result_json as nested JSON: " + e.getMessage());
                }
            } else if (resultJsonNode.isObject()) {
                inner = resultJsonNode;
            } else {
                return failed(loadingId, ParseStatus.RAW_ONLY, "result_json exists but is neither JSON object nor JSON string");
            }

            JsonNode body = inner.path("body");
            if (body.isMissingNode() || body.isNull()) {
                return failed(loadingId, ParseStatus.RAW_ONLY, "body is absent or null");
            }

            if (!body.isObject()) {
                return failed(loadingId, ParseStatus.RAW_ONLY, "body is not a JSON object");
            }

            String entityType = firstText(body, "entity_type", "entityType", "event_type_id", "eventTypeId");
            if (entityType == null || entityType.isBlank()) {
                return failed(loadingId, ParseStatus.RAW_ONLY, "entity_type is absent or blank in body");
            }

            return new ParsedMessage(loadingId, entityType, body, ParseStatus.PARSED, null);
        } catch (Exception e) {
            return failed(null, ParseStatus.INVALID_JSON, "Failed to parse raw JSON: " + e.getMessage());
        }
    }

    public String toJson(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize JSON node: " + e.getMessage(), e);
        }
    }

    public JsonNode parseJson(String bodyJson) {
        try {
            return objectMapper.readTree(bodyJson);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse body_json: " + e.getMessage(), e);
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

    private ParsedMessage failed(String loadingId, ParseStatus status, String errorMessage) {
        return new ParsedMessage(loadingId, null, null, status, errorMessage);
    }
}
