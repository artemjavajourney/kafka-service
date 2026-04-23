package com.example.kafkaservice.apply;

import com.example.kafkaservice.apply.model.EntityOneData;
import com.example.kafkaservice.apply.model.EntityThreeData;
import com.example.kafkaservice.apply.model.EntityTwoData;
import com.example.kafkaservice.intake.RawMessageParser;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
public class BusinessEntityMapper {

    private final RawMessageParser rawMessageParser;

    public EntityOneData toEntityOne(JsonNode body) {
        return new EntityOneData(
                rawMessageParser.firstText(body, "trend_uuid", "trendUuid", "id", "entity_id"),
                rawMessageParser.firstText(body, "trend_name", "trendName"),
                rawMessageParser.firstInt(body, "emotion"),
                rawMessageParser.firstBoolean(body, "is_visible", "isVisible"),
                rawMessageParser.firstInt(body, "product_id", "productId"),
                rawMessageParser.firstText(body, "group_id", "groupId"),
                rawMessageParser.firstBoolean(body, "is_archived", "isArchived"),
                rawMessageParser.firstText(body, "employee_id_create", "employeeIdCreate"),
                parseOffsetDateTime(rawMessageParser.firstText(body, "created_at", "createdAt")),
                rawMessageParser.firstInt(body, "prev_product_id", "prevProductId")
        );
    }

    public EntityTwoData toEntityTwo(JsonNode body) {
        return new EntityTwoData(
                rawMessageParser.firstText(body, "cm_id", "cmId", "id", "entity_id"),
                parseLocalDate(rawMessageParser.firstText(body, "answer_date", "answerDate")),
                parseLocalDate(rawMessageParser.firstText(body, "created_at", "createdAt")),
                rawMessageParser.firstInt(body, "event_type_id", "eventTypeId"),
                rawMessageParser.firstText(body, "trend_uuid", "trendUuid", "entity1_id", "parent_id"),
                rawMessageParser.firstText(body, "summary_uuid", "summaryUuid"),
                rawMessageParser.firstText(body, "client_segment_code", "clientSegmentCode"),
                rawMessageParser.firstInt(body, "product_id", "productId"),
                rawMessageParser.firstInt(body, "prev_product_id", "prevProductId")
        );
    }

    public EntityThreeData toEntityThree(JsonNode body) {
        return new EntityThreeData(
                rawMessageParser.firstText(body, "summary_uuid", "summaryUuid", "id", "entity_id"),
                rawMessageParser.firstText(body, "summary_name", "summaryName"),
                rawMessageParser.firstText(body, "trend_uuid", "trendUuid", "entity1_id", "parent_id"),
                rawMessageParser.firstInt(body, "sentiment"),
                rawMessageParser.firstBoolean(body, "is_etalon", "isEtalon"),
                rawMessageParser.firstText(body, "employee_id_create", "employeeIdCreate"),
                parseOffsetDateTime(rawMessageParser.firstText(body, "created_at", "createdAt"))
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
