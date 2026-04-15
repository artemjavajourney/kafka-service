package com.example.kafkaservice.repository;

import com.example.kafkaservice.audit.EventProcessingLogRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EventProcessingLogRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void insertStagedIfAbsent(EventProcessingLogRecord record) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("stagingId", record.stagingId())
                .addValue("loadingId", record.loadingId())
                .addValue("entityType", record.entityType())
                .addValue("status", record.status().name())
                .addValue("recordsReceived", record.recordsReceived())
                .addValue("recordsInserted", record.recordsInserted())
                .addValue("recordsUpdated", record.recordsUpdated())
                .addValue("recordsSkipped", record.recordsSkipped())
                .addValue("errorMessage", record.errorMessage())
                .addValue("createdAt", record.createdAt())
                .addValue("updatedAt", record.updatedAt());

        jdbcTemplate.update(
                """
                insert into event_processing_log (
                    staging_id,
                    loading_id,
                    entity_type,
                    status,
                    records_received,
                    records_inserted,
                    records_updated,
                    records_skipped,
                    error_message,
                    created_at,
                    updated_at
                ) values (
                    :stagingId,
                    :loadingId,
                    :entityType,
                    :status,
                    :recordsReceived,
                    :recordsInserted,
                    :recordsUpdated,
                    :recordsSkipped,
                    :errorMessage,
                    :createdAt,
                    :updatedAt
                )
                on conflict (staging_id) do nothing
                """,
                params
        );
    }
}
