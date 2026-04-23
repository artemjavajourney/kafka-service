package com.example.kafkaservice.finaltable.repository;

import com.example.kafkaservice.apply.model.EntityTwoData;
import com.example.kafkaservice.apply.model.EntityTwoKey;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class EntityTwoRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void batchUpsert(List<EntityTwoData> records) {
        if (records.isEmpty()) {
            return;
        }

        jdbcTemplate.getJdbcTemplate().batchUpdate(
                """
                insert into final_entity_2 (
                    cm_id,
                    answer_date,
                    created_at,
                    event_type_id,
                    trend_uuid,
                    summary_uuid,
                    client_segment_code,
                    product_id,
                    prev_product_id
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (cm_id, trend_uuid, summary_uuid, answer_date) do update set
                    created_at = excluded.created_at,
                    event_type_id = excluded.event_type_id,
                    client_segment_code = excluded.client_segment_code,
                    product_id = excluded.product_id,
                    prev_product_id = excluded.prev_product_id
                """,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        EntityTwoData record = records.get(i);
                        ps.setString(1, record.cmId());
                        ps.setObject(2, record.answerDate() == null ? null : Date.valueOf(record.answerDate()));
                        ps.setObject(3, record.createdAt() == null ? null : Date.valueOf(record.createdAt()));
                        ps.setObject(4, record.eventTypeId());
                        ps.setString(5, record.trendUuid());
                        ps.setString(6, record.summaryUuid());
                        ps.setString(7, record.clientSegmentCode());
                        ps.setObject(8, record.productId());
                        ps.setObject(9, record.prevProductId());
                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }
                }
        );
    }

    public Map<EntityTwoKey, EntityTwoComparable> findComparableByKeys(Set<EntityTwoKey> keys) {
        if (keys.isEmpty()) {
            return Map.of();
        }

        StringBuilder query = new StringBuilder(
                """
                select cm_id,
                       trend_uuid,
                       summary_uuid,
                       answer_date,
                       created_at,
                       event_type_id,
                       client_segment_code,
                       product_id,
                       prev_product_id
                from final_entity_2
                where
                """
        );

        MapSqlParameterSource params = new MapSqlParameterSource();
        int idx = 0;
        for (EntityTwoKey key : keys) {
            if (idx > 0) {
                query.append(" or ");
            }
            query.append("(cm_id = :cm").append(idx)
                    .append(" and trend_uuid = :tr").append(idx)
                    .append(" and summary_uuid = :su").append(idx)
                    .append(" and answer_date = :ad").append(idx)
                    .append(")");

            params.addValue("cm" + idx, key.cmId());
            params.addValue("tr" + idx, key.trendUuid());
            params.addValue("su" + idx, key.summaryUuid());
            params.addValue("ad" + idx, key.answerDate());
            idx++;
        }

        return jdbcTemplate.query(
                query.toString(),
                params,
                rs -> {
                    Map<EntityTwoKey, EntityTwoComparable> result = new HashMap<>();
                    while (rs.next()) {
                        EntityTwoKey key = new EntityTwoKey(
                                rs.getString("cm_id"),
                                rs.getString("trend_uuid"),
                                rs.getString("summary_uuid"),
                                rs.getDate("answer_date").toLocalDate()
                        );

                        result.put(key, new EntityTwoComparable(
                                rs.getDate("created_at") == null ? null : rs.getDate("created_at").toLocalDate(),
                                (Integer) rs.getObject("event_type_id"),
                                rs.getString("client_segment_code"),
                                (Integer) rs.getObject("product_id"),
                                (Integer) rs.getObject("prev_product_id")
                        ));
                    }
                    return result;
                }
        );
    }

    public record EntityTwoComparable(
            java.time.LocalDate createdAt,
            Integer eventTypeId,
            String clientSegmentCode,
            Integer productId,
            Integer prevProductId
    ) {
        public boolean isChangedComparedTo(EntityTwoData data) {
            return !java.util.Objects.equals(createdAt, data.createdAt())
                    || !java.util.Objects.equals(eventTypeId, data.eventTypeId())
                    || !java.util.Objects.equals(clientSegmentCode, data.clientSegmentCode())
                    || !java.util.Objects.equals(productId, data.productId())
                    || !java.util.Objects.equals(prevProductId, data.prevProductId());
        }
    }
}
