package com.example.kafkaservice.apply;

import java.time.OffsetDateTime;

public record EntityThreeData(
        String summaryUuid,
        String summaryName,
        String trendUuid,
        Integer sentiment,
        Boolean isEtalon,
        String employeeIdCreate,
        OffsetDateTime createdAt
) {
}
