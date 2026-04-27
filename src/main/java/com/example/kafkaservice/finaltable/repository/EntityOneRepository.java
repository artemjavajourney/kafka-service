package com.example.kafkaservice.finaltable.repository;

import com.example.kafkaservice.apply.model.EntityOneData;
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
                ) values (?, ?, ?, coalesce(?, false), ?, ?, coalesce(?, false), ?, coalesce(?, CURRENT_TIMESTAMP), ?)
                on conflict (trend_uuid) do update set
                    trend_name = coalesce(excluded.trend_name, final_entity_1.trend_name),
                    emotion = coalesce(excluded.emotion, final_entity_1.emotion),
                    is_visible = coalesce(excluded.is_visible, final_entity_1.is_archived)
                    product_id = coalesce(excluded.product_id, final_entity_1.product_id),
                    group_id = coalesce(excluded.group_id, final_entity_1.group_id),
                    is_archived = coalesce(excluded.is_archived, final_entity_1.is_archived),
                    employee_id_create = coalesce(excluded.employee_id_create, final_entity_1.employee_id_create),
                    prev_product_id = coalesce(excluded.prev_product_id, final_entity_1.prev_product_id)
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

    public Map<String, EntityOneComparable> findComparableByTrendUuids(Set<String> trendUuids) {
        if (trendUuids.isEmpty()) {
            return Collections.emptyMap();
        }

        return jdbcTemplate.query(
                """
                select trend_uuid,
                       trend_name,
                       emotion,
                       is_visible,
                       product_id,
                       group_id,
                       is_archived,
                       employee_id_create,
                       prev_product_id
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
                                        (Boolean) rs.getObject("is_visible"),
                                        (Integer) rs.getObject("product_id"),
                                        rs.getString("group_id"),
                                        (Boolean) rs.getObject("is_archived"),
                                        rs.getString("employee_id_create"),
                                        (Integer) rs.getObject("prev_product_id")
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
            Boolean isVisible,
            Integer productId,
            String groupId,
            Boolean isArchived,
            String employeeIdCreate,
            Integer prevProductId
    ) {
        public boolean isChangedComparedTo(EntityOneData patch) {
            return isChanged(patch.trendName(), trendName)
                    || isChanged(patch.emotion(), emotion)
                    || isChanged(patch.isVisible(), isVisible)
                    || isChanged(patch.productId(), productId)
                    || isChanged(patch.groupId(), groupId)
                    || isChanged(patch.isArchived(), isArchived)
                    || isChanged(patch.employeeIdCreate(), employeeIdCreate)
                    || isChanged(patch.prevProductId(), prevProductId);
        }

        private static <T> boolean isChanged(T patchValue, T existingValue) {
            return patchValue != null && !java.util.Objects.equals(patchValue, existingValue);
        }
    }
}
