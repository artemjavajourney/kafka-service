package com.example.kafkaservice.apply;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
public class BusinessEntityMapper {

    private final BusinessPayloadExtractor payloadExtractor;

    public EntityOneData toEntityOne(JsonNode body) {
        return new EntityOneData(
                payloadExtractor.firstText(body, "trend_uuid", "trendUuid", "id", "entity_id"),
                payloadExtractor.firstText(body, "trend_name", "trendName"),
                payloadExtractor.firstInt(body, "emotion"),
                payloadExtractor.firstBoolean(body, "is_visible", "isVisible"),
                payloadExtractor.firstInt(body, "product_id", "productId"),
                payloadExtractor.firstText(body, "group_id", "groupId"),
                payloadExtractor.firstBoolean(body, "is_archived", "isArchived"),
                payloadExtractor.firstText(body, "employee_id_create", "employeeIdCreate"),
                parseOffsetDateTime(payloadExtractor.firstText(body, "created_at", "createdAt")),
                payloadExtractor.firstInt(body, "prev_product_id", "prevProductId")
        );
    }

    public EntityTwoData toEntityTwo(JsonNode body) {
        return new EntityTwoData(
                payloadExtractor.firstText(body, "cm_id", "cmId", "id", "entity_id"),
                parseLocalDate(payloadExtractor.firstText(body, "answer_date", "answerDate")),
                parseLocalDate(payloadExtractor.firstText(body, "created_at", "createdAt")),
                payloadExtractor.firstInt(body, "event_type_id", "eventTypeId"),
                payloadExtractor.firstText(body, "trend_uuid", "trendUuid", "entity1_id", "parent_id"),
                payloadExtractor.firstText(body, "summary_uuid", "summaryUuid"),
                payloadExtractor.firstText(body, "client_segment_code", "clientSegmentCode"),
                payloadExtractor.firstInt(body, "product_id", "productId"),
                payloadExtractor.firstInt(body, "prev_product_id", "prevProductId")
        );
    }

    public EntityThreeData toEntityThree(JsonNode body) {
        return new EntityThreeData(
                payloadExtractor.firstText(body, "summary_uuid", "summaryUuid", "id", "entity_id"),
                payloadExtractor.firstText(body, "summary_name", "summaryName"),
                payloadExtractor.firstText(body, "trend_uuid", "trendUuid", "entity1_id", "parent_id"),
                payloadExtractor.firstInt(body, "sentiment"),
                payloadExtractor.firstBoolean(body, "is_etalon", "isEtalon"),
                payloadExtractor.firstText(body, "employee_id_create", "employeeIdCreate"),
                parseOffsetDateTime(payloadExtractor.firstText(body, "created_at", "createdAt"))
        );
    }

    private LocalDate parseLocalDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }

        try {
            return LocalDate.parse(raw);
        } catch (Exception e) {
            return null;
        }
    }

    private OffsetDateTime parseOffsetDateTime(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }

        try {
            return OffsetDateTime.parse(raw);
        } catch (Exception e) {
            return null;
        }
    }
}
