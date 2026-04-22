package com.example.kafkaservice.message;

import com.example.kafkaservice.intake.ParseStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RawMessageParser {

    private final ObjectMapper objectMapper;

    public ParsedRawMessage parse(String rawMessage, String fallbackEntityType) {
        try {
            JsonNode root = objectMapper.readTree(rawMessage);
            String loadingId = firstText(root, "loading_id", "loadingId");

            JsonNode body = NullNode.getInstance();
            ParseStatus parseStatus = ParseStatus.PARSED;
            String errorMessage = null;

            JsonNode resultJsonNode = root.path("result_json");
            if (!resultJsonNode.isMissingNode() && !resultJsonNode.isNull()) {
                JsonNode inner = resultJsonNode;
                if (resultJsonNode.isTextual()) {
                    try {
                        inner = objectMapper.readTree(resultJsonNode.asText());
                    } catch (Exception e) {
                        parseStatus = ParseStatus.RAW_ONLY;
                        errorMessage = "Failed to parse result_json as nested JSON: " + e.getMessage();
                    }
                } else if (!resultJsonNode.isObject()) {
                    parseStatus = ParseStatus.RAW_ONLY;
                    errorMessage = "result_json exists but is neither JSON object nor JSON string";
                }

                if (parseStatus == ParseStatus.PARSED) {
                    body = inner.path("body");
                }
            } else {
                parseStatus = ParseStatus.RAW_ONLY;
                errorMessage = "result_json is absent or null";
            }

            String entityType = firstText(body, "entity_type", "entityType", "event_type_id", "eventTypeId");
            if (entityType == null || entityType.isBlank()) {
                entityType = fallbackEntityType;
            }

            return new ParsedRawMessage(loadingId, entityType, body, parseStatus, errorMessage);
        } catch (Exception e) {
            return new ParsedRawMessage(null, fallbackEntityType, NullNode.getInstance(), ParseStatus.INVALID_JSON, e.getMessage());
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
