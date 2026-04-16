package com.example.kafkaservice.repository;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.audit.EventProcessingLogRecord;
import com.example.kafkaservice.audit.ProcessingLogStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

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

    public List<ApplyCandidate> findForApplyByLoadingId(String loadingId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("loadingId", loadingId);

        return jdbcTemplate.query(
                """
                select epl.staging_id,
                       epl.loading_id,
                       coalesce(epl.entity_type, si.entity_type) as entity_type,
                       si.raw_message
                from event_processing_log epl
                join staging_inbox si on si.id = epl.staging_id
                where epl.loading_id = :loadingId
                  and epl.status in ('STAGED', 'DEFERRED')
                order by
                    case upper(coalesce(epl.entity_type, si.entity_type, ''))
                        when 'ENTITY_1' then 1
                        when 'ENTITY_2' then 2
                        else 100
                    end,
                    epl.staging_id
                """,
                params,
                (rs, rowNum) -> new ApplyCandidate(
                        rs.getLong("staging_id"),
                        rs.getString("loading_id"),
                        rs.getString("entity_type"),
                        rs.getString("raw_message")
                )
        );
    }

    public void updateStatus(long stagingId, ProcessingLogStatus status, String errorMessage, OffsetDateTime updatedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("stagingId", stagingId)
                .addValue("status", status.name())
                .addValue("errorMessage", errorMessage)
                .addValue("updatedAt", updatedAt);

        jdbcTemplate.update(
                """
                update event_processing_log
                set status = :status,
                    error_message = :errorMessage,
                    updated_at = :updatedAt
                where staging_id = :stagingId
                """,
                params
        );
    }
}
