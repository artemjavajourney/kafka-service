package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.apply.BusinessEntityMapper;
import com.example.kafkaservice.apply.model.EntityTwoData;
import com.example.kafkaservice.apply.model.EntityTwoKey;
import com.example.kafkaservice.apply.support.ApplyDedupeUtil;
import com.example.kafkaservice.finaltable.repository.EntityTwoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Order(2)
@RequiredArgsConstructor
public class EntityTwoApplyHandler implements ApplyEntityHandler {

    private final BusinessEntityMapper entityMapper;
    private final EntityTwoRepository entityTwoRepository;

    @Override
    public String supportedType() {
        return "ENTITY_2";
    }

    @Override
    public void handle(List<ApplyHandlerMessage> candidates, List<ApplyStatusUpdate> statusUpdates) {
        if (candidates.isEmpty()) {
            return;
        }

        List<EntityTwoItem> items = new ArrayList<>();
        for (ApplyHandlerMessage message : candidates) {
            EntityTwoData data = entityMapper.toEntityTwo(message.body());
            if (data.cmId() == null || data.cmId().isBlank() || data.trendUuid() == null || data.trendUuid().isBlank()
                    || data.summaryUuid() == null || data.summaryUuid().isBlank() || data.answerDate() == null) {
                statusUpdates.add(ApplyStatusUpdate.deferred(message.stagingId(),
                        "ENTITY_2 message does not contain full business key"));
                continue;
            }

            items.add(new EntityTwoItem(message.stagingId(), data));
        }

        Map<EntityTwoKey, EntityTwoItem> latestByKey = ApplyDedupeUtil.deduplicate(items, EntityTwoItem::key, EntityTwoItem::stagingId, statusUpdates);

        Map<EntityTwoKey, EntityTwoRepository.EntityTwoComparable> existing = entityTwoRepository.findComparableByKeys(latestByKey.keySet());

        List<EntityTwoData> toUpsert = new ArrayList<>();
        List<ApplyStatusUpdate> applyResults = new ArrayList<>();
        for (EntityTwoItem item : latestByKey.values()) {
            EntityTwoRepository.EntityTwoComparable comparable = existing.get(item.key());
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

        entityTwoRepository.batchUpsert(toUpsert);
        statusUpdates.addAll(applyResults);
    }

    private record EntityTwoItem(long stagingId, EntityTwoData data) {
        EntityTwoKey key() {
            return data.key();
        }
    }
}
