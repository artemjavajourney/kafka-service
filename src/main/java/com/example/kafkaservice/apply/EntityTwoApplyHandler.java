package com.example.kafkaservice.apply;

import com.example.kafkaservice.finaltable.EntityOneRepository;
import com.example.kafkaservice.finaltable.EntityTwoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Order(2)
@RequiredArgsConstructor
public class EntityTwoApplyHandler implements ApplyEntityHandler {

    private final BusinessEntityMapper entityMapper;
    private final EntityOneRepository entityOneRepository;
    private final EntityTwoRepository entityTwoRepository;

    @Override
    public String supportedType() {
        return "ENTITY_2";
    }

    @Override
    public void handle(List<ResolvedApplyCandidate> candidates, List<ApplyStatusUpdate> statusUpdates) {
        if (candidates.isEmpty()) {
            return;
        }

        List<EntityTwoItem> items = new ArrayList<>();
        for (ResolvedApplyCandidate resolved : candidates) {
            ApplyCandidate candidate = resolved.candidate();
            EntityTwoData data = entityMapper.toEntityTwo(resolved.payload().body());
            if (data.cmId() == null || data.cmId().isBlank() || data.trendUuid() == null || data.trendUuid().isBlank()
                    || data.summaryUuid() == null || data.summaryUuid().isBlank() || data.answerDate() == null) {
                statusUpdates.add(ApplyStatusUpdate.deferred(candidate.stagingId(),
                        "ENTITY_2 message does not contain full business key"));
                continue;
            }

            items.add(new EntityTwoItem(candidate.stagingId(), data));
        }

        Map<EntityTwoKey, EntityTwoItem> latestByKey = ApplyDedupeUtil.deduplicate(items, EntityTwoItem::key, EntityTwoItem::stagingId, statusUpdates);

        Set<String> trendUuids = latestByKey.values().stream().map(i -> i.data().trendUuid()).collect(Collectors.toSet());
        Set<String> existingParents = entityOneRepository.findExistingTrendUuids(trendUuids);
        Map<EntityTwoKey, EntityTwoRepository.EntityTwoComparable> existing = entityTwoRepository.findComparableByKeys(latestByKey.keySet());

        List<EntityTwoData> toUpsert = new ArrayList<>();
        List<ApplyStatusUpdate> applyResults = new ArrayList<>();
        for (EntityTwoItem item : latestByKey.values()) {
            if (!existingParents.contains(item.data().trendUuid())) {
                statusUpdates.add(ApplyStatusUpdate.deferred(item.stagingId(),
                        "Parent ENTITY_1 not found yet for trend_uuid=" + item.data().trendUuid()));
                continue;
            }

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
