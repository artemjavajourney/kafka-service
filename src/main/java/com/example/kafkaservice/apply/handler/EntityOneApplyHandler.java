package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.apply.BusinessEntityMapper;
import com.example.kafkaservice.apply.model.EntityOneData;
import com.example.kafkaservice.apply.support.ApplyDedupeUtil;
import com.example.kafkaservice.finaltable.repository.EntityOneRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Order(1)
@RequiredArgsConstructor
public class EntityOneApplyHandler implements ApplyEntityHandler {

    private final BusinessEntityMapper entityMapper;
    private final EntityOneRepository entityOneRepository;

    @Override
    public String supportedType() {
        return "ENTITY_1";
    }

    @Override
    public void handle(List<ApplyHandlerMessage> candidates, List<ApplyStatusUpdate> statusUpdates) {
        if (candidates.isEmpty()) {
            return;
        }

        List<EntityOneItem> items = new ArrayList<>();
        for (ApplyHandlerMessage message : candidates) {
            EntityOneData data = entityMapper.toEntityOne(message.body());
            if (data.trendUuid() == null || data.trendUuid().isBlank()) {
                statusUpdates.add(ApplyStatusUpdate.failed(message.stagingId(), "ENTITY_1 message does not contain trend_uuid"));
                continue;
            }

            items.add(new EntityOneItem(message.stagingId(), data));
        }

        Map<String, EntityOneItem> latestByKey = ApplyDedupeUtil.deduplicate(items, EntityOneItem::trendUuid, EntityOneItem::stagingId, statusUpdates);
        Map<String, EntityOneRepository.EntityOneComparable> existing = entityOneRepository.findComparableByTrendUuids(latestByKey.keySet());

        List<EntityOneData> toUpsert = new ArrayList<>();
        List<ApplyStatusUpdate> applyResults = new ArrayList<>();
        for (EntityOneItem item : latestByKey.values()) {
            EntityOneRepository.EntityOneComparable comparable = existing.get(item.trendUuid());
            if (comparable == null) {
                toUpsert.add(item.data());
                applyResults.add(ApplyStatusUpdate.inserted(item.stagingId()));
            } else if (comparable.isChangedComparedTo(item.data())) {
                toUpsert.add(item.data());
                applyResults.add(ApplyStatusUpdate.updated(item.stagingId()));
            } else {
                statusUpdates.add(ApplyStatusUpdate.skipped(item.stagingId(), "No changes"));
            }
        }

        entityOneRepository.batchUpsert(toUpsert);
        statusUpdates.addAll(applyResults);
    }

    private record EntityOneItem(long stagingId, EntityOneData data) {
        String trendUuid() {
            return data.trendUuid();
        }
    }
}
