package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.apply.BusinessEntityMapper;
import com.example.kafkaservice.apply.model.EntityThreeData;
import com.example.kafkaservice.apply.support.ApplyDedupeUtil;
import com.example.kafkaservice.apply.support.ResolvedApplyCandidate;
import com.example.kafkaservice.finaltable.repository.EntityOneRepository;
import com.example.kafkaservice.finaltable.repository.EntityThreeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Order(3)
@RequiredArgsConstructor
public class EntityThreeApplyHandler implements ApplyEntityHandler {

    private final BusinessEntityMapper entityMapper;
    private final EntityOneRepository entityOneRepository;
    private final EntityThreeRepository entityThreeRepository;

    @Override
    public String supportedType() {
        return "ENTITY_3";
    }

    @Override
    public void handle(List<ResolvedApplyCandidate> candidates, List<ApplyStatusUpdate> statusUpdates) {
        if (candidates.isEmpty()) {
            return;
        }

        List<EntityThreeItem> items = new ArrayList<>();
        for (ResolvedApplyCandidate resolved : candidates) {
            ApplyCandidate candidate = resolved.candidate();
            EntityThreeData data = entityMapper.toEntityThree(resolved.payload().body());
            if (data.summaryUuid() == null || data.summaryUuid().isBlank() || data.trendUuid() == null || data.trendUuid().isBlank()) {
                statusUpdates.add(ApplyStatusUpdate.deferred(candidate.stagingId(),
                        "ENTITY_3 message does not contain full business key"));
                continue;
            }

            items.add(new EntityThreeItem(candidate.stagingId(), data));
        }

        Map<String, EntityThreeItem> latestByKey = ApplyDedupeUtil.deduplicate(items, EntityThreeItem::summaryUuid, EntityThreeItem::stagingId, statusUpdates);
        Set<String> trendUuids = latestByKey.values().stream().map(i -> i.data().trendUuid()).collect(Collectors.toSet());
        Set<String> existingParents = entityOneRepository.findExistingTrendUuids(trendUuids);
        Map<String, EntityThreeRepository.EntityThreeComparable> existing =
                entityThreeRepository.findComparableBySummaryUuids(latestByKey.keySet());

        List<EntityThreeData> toUpsert = new ArrayList<>();
        List<ApplyStatusUpdate> applyResults = new ArrayList<>();
        for (EntityThreeItem item : latestByKey.values()) {
            if (!existingParents.contains(item.data().trendUuid())) {
                statusUpdates.add(ApplyStatusUpdate.deferred(item.stagingId(),
                        "Parent ENTITY_1 not found yet for trend_uuid=" + item.data().trendUuid()));
                continue;
            }

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
