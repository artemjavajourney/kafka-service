package com.example.kafkaservice.finaltable.repository;

import com.example.kafkaservice.apply.model.EntityThreeData;
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
public class EntityThreeRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void batchUpsert(List<EntityThreeData> records) {
        if (records.isEmpty()) {
            return;
        }

        jdbcTemplate.getJdbcTemplate().batchUpdate(
                """
                insert into final_entity_3 (
                    summary_uuid,
                    summary_name,
                    trend_uuid,
                    sentiment,
                    is_etalon,
                    employee_id_create,
                    created_at
                ) values (?, ?, ?, ?, coalesce(?, false) ?, coalesce(?, CURRENT_TIMESTAMP))
                on conflict (summary_uuid) do update set
                    summary_name = coalesce(excluded.summary_name, final_entity_1.summary_name),
                    trend_uuid = coalesce(excluded.trend_uuid,  final_entity_1.trend_uuid),
                    sentiment = coalesce(excluded.sentiment,  final_entity_1.sentiment),
                    is_etalon = coalesce(excluded.is_etalon,  final_entity_1.is_etalon)
                    employee_id_create = coalesce(excluded.employee_id_create,   final_entity_1.employee_id_create),
                """,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        EntityThreeData record = records.get(i);
                        ps.setString(1, record.summaryUuid());
                        ps.setString(2, record.summaryName());
                        ps.setString(3, record.trendUuid());
                        ps.setObject(4, record.sentiment());
                        ps.setObject(5, record.isEtalon());
                        ps.setString(6, record.employeeIdCreate());
                        ps.setTimestamp(7, record.createdAt() == null ? null : Timestamp.from(record.createdAt().toInstant()));
                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }
                }
        );
    }

    public Map<String, EntityThreeComparable> findComparableBySummaryUuids(Set<String> summaryUuids) {
        if (summaryUuids.isEmpty()) {
            return Collections.emptyMap();
        }

        return jdbcTemplate.query(
                """
                select summary_uuid,
                       summary_name,
                       trend_uuid,
                       sentiment,
                       is_etalon,
                       employee_id_create,
                from final_entity_3
                where summary_uuid in (:ids)
                """,
                new MapSqlParameterSource("ids", summaryUuids),
                rs -> {
                    Map<String, EntityThreeComparable> result = new HashMap<>();
                    while (rs.next()) {
                        result.put(
                                rs.getString("summary_uuid"),
                                new EntityThreeComparable(
                                        rs.getString("summary_name"),
                                        rs.getString("trend_uuid"),
                                        (Integer) rs.getObject("sentiment"),
                                        (Boolean) rs.getObject("is_etalon"),
                                        rs.getString("employee_id_create")
                                )
                        );
                    }
                    return result;
                }
        );
    }

    public record EntityThreeComparable(
            String summaryName,
            String trendUuid,
            Integer sentiment,
            Boolean isEtalon,
            String employeeIdCreate
    ) {
        public boolean isChangedComparedTo(EntityThreeData data) {
            return isChanged(summaryName, data.summaryName())
                    || isChanged(trendUuid, data.trendUuid())
                    || isChanged(sentiment, data.sentiment())
                    || isChanged(isEtalon, data.isEtalon())
                    || isChanged(employeeIdCreate, data.employeeIdCreate());
        }

        private static <T> boolean isChanged(T patchValue, T existingValue) {
            return patchValue != null && !java.util.Objects.equals(patchValue, existingValue);
        }
    }
}
