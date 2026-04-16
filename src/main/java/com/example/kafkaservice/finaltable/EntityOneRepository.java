package com.example.kafkaservice.finaltable;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EntityOneRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void upsert(String businessId, String payload) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("businessId", businessId)
                .addValue("payload", payload);

        jdbcTemplate.update(
                """
                insert into final_entity_1 (business_id, payload)
                values (:businessId, cast(:payload as jsonb))
                on conflict (business_id) do update set payload = excluded.payload
                """,
                params
        );
    }

    public boolean exists(String businessId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("businessId", businessId);

        Boolean exists = jdbcTemplate.queryForObject(
                """
                select exists(
                    select 1
                    from final_entity_1
                    where business_id = :businessId
                )
                """,
                params,
                Boolean.class
        );

        return Boolean.TRUE.equals(exists);
    }
}
