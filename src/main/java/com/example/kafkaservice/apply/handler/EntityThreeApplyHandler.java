package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.apply.BusinessEntityMapper;
import com.example.kafkaservice.apply.model.EntityThreeData;
import com.example.kafkaservice.apply.support.ApplyDedupeUtil;
import com.example.kafkaservice.finaltable.repository.EntityThreeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Order(3)
@RequiredArgsConstructor
public class EntityThreeApplyHandler implements ApplyEntityHandler {

    private final BusinessEntityMapper entityMapper;
    private final EntityThreeRepository entityThreeRepository;

    @Override
    public String supportedType() {
        return "ENTITY_3";
    }

    @Override
    public void handle(List<ApplyCandidate> candidates, List<ApplyStatusUpdate> statusUpdates) {
        if (candidates.isEmpty()) {
            return;
        }

        List<EntityThreeItem> items = new ArrayList<>();
        for (ApplyCandidate candidate : candidates) {
            EntityThreeData data = entityMapper.toEntityThree(candidate.body());
            if (data.summaryUuid() == null || data.summaryUuid().isBlank()
                    || data.trendUuid() == null || data.trendUuid().isBlank()) {
                statusUpdates.add(ApplyStatusUpdate.failed(candidate.stagingId(),
                        "ENTITY_3 message does not contain summary_uuid and trend_uuid"));
                continue;
            }

            items.add(new EntityThreeItem(candidate.stagingId(), data));
        }

        Map<String, EntityThreeItem> latestByKey = ApplyDedupeUtil.deduplicate(items, EntityThreeItem::summaryUuid, EntityThreeItem::stagingId, statusUpdates);
        Map<String, EntityThreeRepository.EntityThreeComparable> existing =
                entityThreeRepository.findComparableBySummaryUuids(latestByKey.keySet());

        List<EntityThreeData> toUpsert = new ArrayList<>();
        List<ApplyStatusUpdate> applyResults = new ArrayList<>();
        for (EntityThreeItem item : latestByKey.values()) {
            EntityThreeRepository.EntityThreeComparable comparable = existing.get(item.summaryUuid());
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

        entityThreeRepository.batchUpsert(toUpsert);
        statusUpdates.addAll(applyResults);
    }

    private record EntityThreeItem(long stagingId, EntityThreeData data) {
        String summaryUuid() {
            return data.summaryUuid();
        }
    }
}
