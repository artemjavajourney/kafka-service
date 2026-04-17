package com.example.kafkaservice.apply;

import com.example.kafkaservice.audit.ProcessingLogStatus;
import com.example.kafkaservice.finaltable.EntityOneRepository;
import com.example.kafkaservice.finaltable.EntityThreeRepository;
import com.example.kafkaservice.finaltable.EntityTwoRepository;
import com.example.kafkaservice.repository.EventProcessingLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ApplyOrchestrator {

    private static final int DEFAULT_BATCH_SIZE = 500;

    private final EventProcessingLogRepository eventProcessingLogRepository;
    private final BusinessPayloadExtractor businessPayloadExtractor;
    private final BusinessEntityMapper entityMapper;
    private final EntityOneRepository entityOneRepository;
    private final EntityTwoRepository entityTwoRepository;
    private final EntityThreeRepository entityThreeRepository;

    @Transactional
    public int applyNextBatch() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        List<ApplyCandidate> candidates = eventProcessingLogRepository.claimNextBatch(DEFAULT_BATCH_SIZE, now);
        if (candidates.isEmpty()) {
            return 0;
        }

        List<ApplyStatusUpdate> statusUpdates = new ArrayList<>();
        List<EntityOneItem> entityOneItems = new ArrayList<>();
        List<EntityTwoItem> entityTwoItems = new ArrayList<>();
        List<EntityThreeItem> entityThreeItems = new ArrayList<>();

        for (ApplyCandidate candidate : candidates) {
            try {
                BusinessPayload payload = businessPayloadExtractor.extract(candidate.rawMessage(), candidate.entityType());
                String normalizedType = normalizeType(payload.entityType());

                if ("ENTITY_1".equals(normalizedType)) {
                    collectEntityOne(candidate, payload, statusUpdates, entityOneItems);
                } else if ("ENTITY_2".equals(normalizedType)) {
                    collectEntityTwo(candidate, payload, statusUpdates, entityTwoItems);
                } else if ("ENTITY_3".equals(normalizedType)) {
                    collectEntityThree(candidate, payload, statusUpdates, entityThreeItems);
                } else {
                    statusUpdates.add(ApplyStatusUpdate.skipped(candidate.stagingId(), "Unknown entity type: " + payload.entityType()));
                }
            } catch (Exception e) {
                statusUpdates.add(ApplyStatusUpdate.failed(candidate.stagingId(),
                        "Failed to parse raw message for apply: " + e.getMessage()));
            }
        }

        applyEntityOneBatch(entityOneItems, statusUpdates);
        applyEntityTwoBatch(entityTwoItems, statusUpdates);
        applyEntityThreeBatch(entityThreeItems, statusUpdates);

        eventProcessingLogRepository.batchUpdateStatuses(statusUpdates, now);

        long applied = statusUpdates.stream().filter(s -> s.status() == ProcessingLogStatus.APPLIED).count();
        long deferred = statusUpdates.stream().filter(s -> s.status() == ProcessingLogStatus.DEFERRED).count();

        log.info("Apply batch finished: picked={}, applied={}, deferred={}, other={}",
                candidates.size(),
                applied,
                deferred,
                statusUpdates.size() - applied - deferred);

        return candidates.size();
    }

    private void applyEntityOneBatch(List<EntityOneItem> items, List<ApplyStatusUpdate> statusUpdates) {
        if (items.isEmpty()) {
            return;
        }

        Map<String, EntityOneItem> latestByKey = deduplicate(items, EntityOneItem::trendUuid, statusUpdates);
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

    private void applyEntityTwoBatch(List<EntityTwoItem> items, List<ApplyStatusUpdate> statusUpdates) {
        if (items.isEmpty()) {
            return;
        }

        Map<EntityTwoKey, EntityTwoItem> latestByKey = deduplicate(items, EntityTwoItem::key, statusUpdates);

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

    private void applyEntityThreeBatch(List<EntityThreeItem> items, List<ApplyStatusUpdate> statusUpdates) {
        if (items.isEmpty()) {
            return;
        }

        Map<String, EntityThreeItem> latestByKey = deduplicate(items, EntityThreeItem::summaryUuid, statusUpdates);
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

    private <K, T extends StagingAware<K>> Map<K, T> deduplicate(List<T> items, java.util.function.Function<T, K> keyExtractor,
                                                                  List<ApplyStatusUpdate> statusUpdates) {
        Map<K, T> latest = new LinkedHashMap<>();
        for (T item : items) {
            latest.put(keyExtractor.apply(item), item);
        }

        Set<Long> latestIds = latest.values().stream().map(StagingAware::stagingId).collect(Collectors.toSet());
        items.stream()
                .map(StagingAware::stagingId)
                .filter(id -> !latestIds.contains(id))
                .forEach(id -> statusUpdates.add(ApplyStatusUpdate.skipped(id, "Obsolete version in batch")));

        return latest;
    }

    private void collectEntityOne(ApplyCandidate candidate, BusinessPayload payload, List<ApplyStatusUpdate> statusUpdates,
                                  List<EntityOneItem> items) {
        EntityOneData data = entityMapper.toEntityOne(payload.body());
        if (data.trendUuid() == null || data.trendUuid().isBlank()) {
            statusUpdates.add(ApplyStatusUpdate.failed(candidate.stagingId(), "ENTITY_1 message does not contain trend_uuid"));
            return;
        }

        items.add(new EntityOneItem(candidate.stagingId(), data));
    }

    private void collectEntityTwo(ApplyCandidate candidate, BusinessPayload payload, List<ApplyStatusUpdate> statusUpdates,
                                  List<EntityTwoItem> items) {
        EntityTwoData data = entityMapper.toEntityTwo(payload.body());
        if (data.cmId() == null || data.cmId().isBlank() || data.trendUuid() == null || data.trendUuid().isBlank()
                || data.summaryUuid() == null || data.summaryUuid().isBlank() || data.answerDate() == null) {
            statusUpdates.add(ApplyStatusUpdate.deferred(candidate.stagingId(),
                    "ENTITY_2 message does not contain full business key"));
            return;
        }

        items.add(new EntityTwoItem(candidate.stagingId(), data));
    }

    private void collectEntityThree(ApplyCandidate candidate, BusinessPayload payload, List<ApplyStatusUpdate> statusUpdates,
                                    List<EntityThreeItem> items) {
        EntityThreeData data = entityMapper.toEntityThree(payload.body());
        if (data.summaryUuid() == null || data.summaryUuid().isBlank()) {
            statusUpdates.add(ApplyStatusUpdate.failed(candidate.stagingId(), "ENTITY_3 message does not contain summary_uuid"));
            return;
        }

        items.add(new EntityThreeItem(candidate.stagingId(), data));
    }

    private String normalizeType(String entityType) {
        if (entityType == null) {
            return "";
        }

        return entityType.trim().toUpperCase();
    }

    private interface StagingAware<K> {
        long stagingId();
    }

    private record EntityOneItem(long stagingId, EntityOneData data) implements StagingAware<String> {
        String trendUuid() {
            return data.trendUuid();
        }
    }

    private record EntityTwoItem(long stagingId, EntityTwoData data) implements StagingAware<EntityTwoKey> {
        EntityTwoKey key() {
            return data.key();
        }
    }

    private record EntityThreeItem(long stagingId, EntityThreeData data) implements StagingAware<String> {
        String summaryUuid() {
            return data.summaryUuid();
        }
    }
}
