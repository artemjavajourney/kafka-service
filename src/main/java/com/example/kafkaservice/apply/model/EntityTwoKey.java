package com.example.kafkaservice.apply.model;

import java.time.LocalDate;

public record EntityTwoKey(
        String cmId,
        String trendUuid,
        String summaryUuid,
        LocalDate answerDate
) {
}
