package com.example.kafkaservice.apply;

import java.time.LocalDate;

public record EntityTwoKey(
        String cmId,
        String trendUuid,
        String summaryUuid,
        LocalDate answerDate
) {
}
