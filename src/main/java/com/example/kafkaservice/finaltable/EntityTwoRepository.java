package com.example.kafkaservice.finaltable;

import com.example.kafkaservice.apply.FinalUpsertItem;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class EntityTwoRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void batchUpsert(List<FinalUpsertItem> records) {
        if (records.isEmpty()) {
            return;
        }

        jdbcTemplate.getJdbcTemplate().batchUpdate(
                """
                insert into final_entity_2 (business_id, entity1_business_id, payload)
                values (?, ?, cast(? as jsonb))
                on conflict (business_id) do update set
                    entity1_business_id = excluded.entity1_business_id,
                    payload = excluded.payload
                """,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        FinalUpsertItem record = records.get(i);
                        ps.setString(1, record.businessId());
                        ps.setString(2, record.parentBusinessId());
                        ps.setString(3, record.payload());
                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }
                }
        );
    }

    public Map<String, String> findPayloadByBusinessIds(Set<String> businessIds) {
        if (businessIds.isEmpty()) {
            return Collections.emptyMap();
        }

        return jdbcTemplate.query(
                """
                select business_id,
                       payload::text as payload
                from final_entity_2
                where business_id in (:ids)
                """,
                new MapSqlParameterSource("ids", businessIds),
                rs -> {
                    Map<String, String> result = new HashMap<>();
                    while (rs.next()) {
                        result.put(rs.getString("business_id"), rs.getString("payload"));
                    }
                    return result;
                }
        );
    }
}
