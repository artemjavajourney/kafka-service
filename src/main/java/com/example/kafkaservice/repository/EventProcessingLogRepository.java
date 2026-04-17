package com.example.kafkaservice.repository;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.audit.EventProcessingLogRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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

    public List<ApplyCandidate> claimNextBatch(int batchSize, OffsetDateTime now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("batchSize", batchSize)
                .addValue("now", now)
                .addValue("processingStatus", "PROCESSING");

        return jdbcTemplate.query(
                """
                with picked as (
                    select epl.staging_id
                    from event_processing_log epl
                    where epl.status in ('STAGED', 'DEFERRED')
                    order by epl.created_at, epl.staging_id
                    limit :batchSize
                    for update skip locked
                ), marked as (
                    update event_processing_log epl
                    set status = :processingStatus,
                        updated_at = :now,
                        error_message = null,
                        records_inserted = 0,
                        records_updated = 0,
                        records_skipped = 0
                    from picked
                    where epl.staging_id = picked.staging_id
                    returning epl.staging_id,
                              epl.loading_id,
                              epl.entity_type
                )
                select marked.staging_id,
                       marked.loading_id,
                       marked.entity_type,
                       si.raw_message
                from marked
                join staging_inbox si on si.id = marked.staging_id
                order by marked.staging_id
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

    public void batchUpdateStatuses(List<ApplyStatusUpdate> updates, OffsetDateTime now) {
        if (updates.isEmpty()) {
            return;
        }

        jdbcTemplate.getJdbcTemplate().batchUpdate(
                """
                update event_processing_log
                set status = ?,
                    error_message = ?,
                    records_inserted = ?,
                    records_updated = ?,
                    records_skipped = ?,
                    updated_at = ?
                where staging_id = ?
                """,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ApplyStatusUpdate update = updates.get(i);
                        ps.setString(1, update.status().name());
                        ps.setString(2, update.errorMessage());
                        ps.setInt(3, update.recordsInserted());
                        ps.setInt(4, update.recordsUpdated());
                        ps.setInt(5, update.recordsSkipped());
                        ps.setTimestamp(6, Timestamp.from(now.toInstant()));
                        ps.setLong(7, update.stagingId());
                    }

                    @Override
                    public int getBatchSize() {
                        return updates.size();
                    }
                }
        );
    }
}
