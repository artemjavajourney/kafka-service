package com.example.kafkaservice.finaltable;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EntityTwoRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void upsert(String businessId, String parentBusinessId, String payload) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("businessId", businessId)
                .addValue("parentBusinessId", parentBusinessId)
                .addValue("payload", payload);

        jdbcTemplate.update(
                """
                insert into final_entity_2 (business_id, entity1_business_id, payload)
                values (:businessId, :parentBusinessId, cast(:payload as jsonb))
                on conflict (business_id) do update set
                    entity1_business_id = excluded.entity1_business_id,
                    payload = excluded.payload
                """,
                params
        );
    }
}
