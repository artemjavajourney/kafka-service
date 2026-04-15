package com.example.kafkaservice.repository;

import com.example.kafkaservice.intake.StagingInboxRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class StagingInboxRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public long insertIfAbsent(StagingInboxRecord record) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("kafkaTopic", record.kafkaTopic())
                .addValue("kafkaPartition", record.kafkaPartition())
                .addValue("kafkaOffset", record.kafkaOffset())
                .addValue("kafkaKey", record.kafkaKey())
                .addValue("loadingId", record.loadingId())
                .addValue("entityType", record.entityType())
                .addValue("rawMessage", record.rawMessage())
                .addValue("parseStatus", record.parseStatus().name())
                .addValue("intakeStatus", record.intakeStatus().name())
                .addValue("errorMessage", record.errorMessage())
                .addValue("receivedAt", record.receivedAt())
                .addValue("stagedAt", record.stagedAt());

        Long insertedId = jdbcTemplate.query(
                """
                insert into staging_inbox (
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    kafka_key,
                    loading_id,
                    entity_type,
                    raw_message,
                    parse_status,
                    intake_status,
                    error_message,
                    received_at,
                    staged_at
                ) values (
                    :kafkaTopic,
                    :kafkaPartition,
                    :kafkaOffset,
                    :kafkaKey,
                    :loadingId,
                    :entityType,
                    :rawMessage,
                    :parseStatus,
                    :intakeStatus,
                    :errorMessage,
                    :receivedAt,
                    :stagedAt
                )
                on conflict (kafka_topic, kafka_partition, kafka_offset) do nothing
                returning id
                """,
                params,
                rs -> rs.next() ? rs.getLong("id") : null
        );

        if (insertedId != null) {
            return insertedId;
        }

        return jdbcTemplate.queryForObject(
                """
                select id
                from staging_inbox
                where kafka_topic = :kafkaTopic
                  and kafka_partition = :kafkaPartition
                  and kafka_offset = :kafkaOffset
                """,
                params,
                Long.class
        );
    }
}
