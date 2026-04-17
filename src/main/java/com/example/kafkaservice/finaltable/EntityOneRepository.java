package com.example.kafkaservice.finaltable;

import com.example.kafkaservice.apply.EntityOneData;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class EntityOneRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void batchUpsert(List<EntityOneData> records) {
        if (records.isEmpty()) {
            return;
        }

        jdbcTemplate.getJdbcTemplate().batchUpdate(
                """
                insert into final_entity_1 (
                    trend_uuid,
                    trend_name,
                    emotion,
                    is_visible,
                    product_id,
                    group_id,
                    is_archived,
                    employee_id_create,
                    created_at,
                    prev_product_id
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (trend_uuid) do update set
                    trend_name = excluded.trend_name,
                    emotion = excluded.emotion,
                    is_visible = excluded.is_visible,
                    product_id = excluded.product_id,
                    group_id = excluded.group_id,
                    is_archived = excluded.is_archived,
                    employee_id_create = excluded.employee_id_create,
                    created_at = excluded.created_at,
                    prev_product_id = excluded.prev_product_id
                """,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        EntityOneData record = records.get(i);
                        ps.setString(1, record.trendUuid());
                        ps.setString(2, record.trendName());
                        ps.setObject(3, record.emotion());
                        ps.setObject(4, record.isVisible());
                        ps.setObject(5, record.productId());
                        ps.setString(6, record.groupId());
                        ps.setObject(7, record.isArchived());
                        ps.setString(8, record.employeeIdCreate());
                        ps.setTimestamp(9, record.createdAt() == null ? null : Timestamp.from(record.createdAt().toInstant()));
                        ps.setObject(10, record.prevProductId());
                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }
                }
        );
    }

    public Set<String> findExistingTrendUuids(Set<String> trendUuids) {
        if (trendUuids.isEmpty()) {
            return Collections.emptySet();
        }

        return Set.copyOf(jdbcTemplate.queryForList(
                """
                select trend_uuid
                from final_entity_1
                where trend_uuid in (:ids)
                """,
                new MapSqlParameterSource("ids", trendUuids),
                String.class
        ));
    }

    public Map<String, EntityOneComparable> findComparableByTrendUuids(Set<String> trendUuids) {
        if (trendUuids.isEmpty()) {
            return Collections.emptyMap();
        }

        return jdbcTemplate.query(
                """
                select trend_uuid,
                       trend_name,
                       emotion,
                       product_id
                from final_entity_1
                where trend_uuid in (:ids)
                """,
                new MapSqlParameterSource("ids", trendUuids),
                rs -> {
                    Map<String, EntityOneComparable> result = new HashMap<>();
                    while (rs.next()) {
                        result.put(
                                rs.getString("trend_uuid"),
                                new EntityOneComparable(
                                        rs.getString("trend_name"),
                                        (Integer) rs.getObject("emotion"),
                                        (Integer) rs.getObject("product_id")
                                )
                        );
                    }
                    return result;
                }
        );
    }

    public record EntityOneComparable(
            String trendName,
            Integer emotion,
            Integer productId
    ) {
        public boolean isChangedComparedTo(EntityOneData data) {
            return !java.util.Objects.equals(trendName, data.trendName())
                    || !java.util.Objects.equals(emotion, data.emotion())
                    || !java.util.Objects.equals(productId, data.productId());
        }
    }
}
