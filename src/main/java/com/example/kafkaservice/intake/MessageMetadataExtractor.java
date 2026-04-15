package com.example.kafkaservice.intake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MessageMetadataExtractor {

    private final ObjectMapper objectMapper;

    public ExtractedMetadata extract(String rawMessage) {
        try {
            JsonNode root = objectMapper.readTree(rawMessage);

            String loadingId = firstText(root, "loading_id", "loadingId");
            JsonNode resultJsonNode = root.path("result_json");
            JsonNode inner = null;
            ParseStatus parseStatus = ParseStatus.PARSED;
            String errorMessage = null;

            if (!resultJsonNode.isMissingNode() && !resultJsonNode.isNull()) {
                if (resultJsonNode.isTextual()) {
                    try {
                        inner = objectMapper.readTree(resultJsonNode.asText());
                    } catch (Exception e) {
                        parseStatus = ParseStatus.RAW_ONLY;
                        errorMessage = "Failed to parse result_json as nested JSON: " + e.getMessage();
                    }
                } else if (resultJsonNode.isObject()) {
                    inner = resultJsonNode;
                } else {
                    parseStatus = ParseStatus.RAW_ONLY;
                    errorMessage = "result_json exists but is neither JSON object nor JSON string";
                }
            } else {
                parseStatus = ParseStatus.RAW_ONLY;
                errorMessage = "result_json is absent or null";
            }

            String entityType = null;
            if (inner != null) {
                JsonNode body = inner.path("body");
                entityType = firstText(body, "entity_type", "entityType", "event_type_id", "eventTypeId");
            }

            return new ExtractedMetadata(
                    loadingId,
                    entityType,
                    parseStatus,
                    errorMessage
            );

        } catch (Exception e) {
            return new ExtractedMetadata(
                    null,
                    null,
                    ParseStatus.INVALID_JSON,
                    e.getMessage()
            );
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
