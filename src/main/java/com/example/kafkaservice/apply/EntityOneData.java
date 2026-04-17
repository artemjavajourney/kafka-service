package com.example.kafkaservice.apply;

import java.time.OffsetDateTime;

public record EntityOneData(
        String trendUuid,
        String trendName,
        Integer emotion,
        Boolean isVisible,
        Integer productId,
        String groupId,
        Boolean isArchived,
        String employeeIdCreate,
        OffsetDateTime createdAt,
        Integer prevProductId
) {
}
