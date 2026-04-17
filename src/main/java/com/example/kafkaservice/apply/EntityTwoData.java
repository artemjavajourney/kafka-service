package com.example.kafkaservice.apply;

import java.time.LocalDate;

public record EntityTwoData(
        String cmId,
        LocalDate answerDate,
        LocalDate createdAt,
        Integer eventTypeId,
        String trendUuid,
        String summaryUuid,
        String clientSegmentCode,
        Integer productId,
        Integer prevProductId
) {
    public EntityTwoKey key() {
        return new EntityTwoKey(cmId, trendUuid, summaryUuid, answerDate);
    }
}
