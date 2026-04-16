package com.example.kafkaservice.apply;

import com.example.kafkaservice.audit.ProcessingLogStatus;
import com.example.kafkaservice.finaltable.EntityOneRepository;
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
    private final EntityOneRepository entityOneRepository;
    private final EntityTwoRepository entityTwoRepository;

    @Transactional
    public int applyNextBatch() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        List<ApplyCandidate> candidates = eventProcessingLogRepository.claimNextBatch(DEFAULT_BATCH_SIZE, now);
        if (candidates.isEmpty()) {
            return 0;
        }

        List<ApplyStatusUpdate> statusUpdates = new ArrayList<>();
        List<FinalUpsertItem> entityOneRecords = new ArrayList<>();
        List<FinalUpsertItem> entityTwoRecords = new ArrayList<>();

        for (ApplyCandidate candidate : candidates) {
            try {
                BusinessPayload payload = businessPayloadExtractor.extract(candidate.rawMessage(), candidate.entityType());
                String normalizedType = normalizeType(payload.entityType());

                if ("ENTITY_1".equals(normalizedType)) {
                    collectEntityOne(candidate, payload, statusUpdates, entityOneRecords);
                } else if ("ENTITY_2".equals(normalizedType)) {
                    collectEntityTwo(candidate, payload, statusUpdates, entityTwoRecords);
                } else {
                    statusUpdates.add(new ApplyStatusUpdate(candidate.stagingId(), ProcessingLogStatus.SKIPPED,
                            "Unknown entity type: " + payload.entityType()));
                }
            } catch (Exception e) {
                statusUpdates.add(new ApplyStatusUpdate(candidate.stagingId(), ProcessingLogStatus.FAILED,
                        "Failed to parse raw message for apply: " + e.getMessage()));
            }
        }

        applyEntityOneBatch(entityOneRecords, statusUpdates);
        applyEntityTwoBatch(entityTwoRecords, statusUpdates);
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

    private void applyEntityOneBatch(List<FinalUpsertItem> records, List<ApplyStatusUpdate> statusUpdates) {
        if (records.isEmpty()) {
            return;
        }

        DedupResult dedup = deduplicateByBusinessId(records);
        dedup.obsoleteStagingIds().forEach(id -> statusUpdates.add(new ApplyStatusUpdate(id, ProcessingLogStatus.SKIPPED, "Obsolete version in batch")));

        Map<String, String> existingPayloads = entityOneRepository.findPayloadByBusinessIds(dedup.latestByBusinessId().keySet());

        List<FinalUpsertItem> toUpsert = new ArrayList<>();
        for (FinalUpsertItem record : dedup.latestByBusinessId().values()) {
            String existingPayload = existingPayloads.get(record.businessId());
            if (existingPayload != null && existingPayload.equals(record.payload())) {
                statusUpdates.add(new ApplyStatusUpdate(record.stagingId(), ProcessingLogStatus.SKIPPED, "No changes"));
            } else {
                toUpsert.add(record);
            }
        }

        entityOneRepository.batchUpsert(toUpsert);
        toUpsert.forEach(record -> statusUpdates.add(new ApplyStatusUpdate(record.stagingId(), ProcessingLogStatus.APPLIED, null)));
    }

    private void applyEntityTwoBatch(List<FinalUpsertItem> records, List<ApplyStatusUpdate> statusUpdates) {
        if (records.isEmpty()) {
            return;
        }

        DedupResult dedup = deduplicateByBusinessId(records);
        dedup.obsoleteStagingIds().forEach(id -> statusUpdates.add(new ApplyStatusUpdate(id, ProcessingLogStatus.SKIPPED, "Obsolete version in batch")));

        Set<String> parentIds = dedup.latestByBusinessId().values().stream()
                .map(FinalUpsertItem::parentBusinessId)
                .collect(Collectors.toSet());

        Set<String> existingParents = entityOneRepository.findExistingBusinessIds(parentIds);
        Map<String, String> existingPayloads = entityTwoRepository.findPayloadByBusinessIds(dedup.latestByBusinessId().keySet());

        List<FinalUpsertItem> toUpsert = new ArrayList<>();
        for (FinalUpsertItem record : dedup.latestByBusinessId().values()) {
            if (!existingParents.contains(record.parentBusinessId())) {
                statusUpdates.add(new ApplyStatusUpdate(record.stagingId(), ProcessingLogStatus.DEFERRED,
                        "Parent ENTITY_1 not found yet for id=" + record.parentBusinessId()));
                continue;
            }

            String existingPayload = existingPayloads.get(record.businessId());
            if (existingPayload != null && existingPayload.equals(record.payload())) {
                statusUpdates.add(new ApplyStatusUpdate(record.stagingId(), ProcessingLogStatus.SKIPPED, "No changes"));
                continue;
            }

            toUpsert.add(record);
        }

        entityTwoRepository.batchUpsert(toUpsert);
        toUpsert.forEach(record -> statusUpdates.add(new ApplyStatusUpdate(record.stagingId(), ProcessingLogStatus.APPLIED, null)));
    }

    private DedupResult deduplicateByBusinessId(List<FinalUpsertItem> records) {
        Map<String, FinalUpsertItem> latest = new LinkedHashMap<>();
        for (FinalUpsertItem record : records) {
            latest.put(record.businessId(), record);
        }

        Set<Long> latestStagingIds = latest.values().stream().map(FinalUpsertItem::stagingId).collect(Collectors.toSet());
        List<Long> obsolete = records.stream()
                .map(FinalUpsertItem::stagingId)
                .filter(id -> !latestStagingIds.contains(id))
                .toList();

        return new DedupResult(latest, obsolete);
    }

    private void collectEntityOne(
            ApplyCandidate candidate,
            BusinessPayload payload,
            List<ApplyStatusUpdate> statusUpdates,
            List<FinalUpsertItem> records
    ) {
        if (payload.businessId() == null || payload.businessId().isBlank()) {
            statusUpdates.add(new ApplyStatusUpdate(candidate.stagingId(), ProcessingLogStatus.FAILED,
                    "ENTITY_1 message does not contain business id"));
            return;
        }

        records.add(new FinalUpsertItem(candidate.stagingId(), payload.businessId(), null, payload.rawBody()));
    }

    private void collectEntityTwo(
            ApplyCandidate candidate,
            BusinessPayload payload,
            List<ApplyStatusUpdate> statusUpdates,
            List<FinalUpsertItem> records
    ) {
        if (payload.businessId() == null || payload.businessId().isBlank()) {
            statusUpdates.add(new ApplyStatusUpdate(candidate.stagingId(), ProcessingLogStatus.FAILED,
                    "ENTITY_2 message does not contain business id"));
            return;
        }

        if (payload.parentBusinessId() == null || payload.parentBusinessId().isBlank()) {
            statusUpdates.add(new ApplyStatusUpdate(candidate.stagingId(), ProcessingLogStatus.DEFERRED,
                    "ENTITY_2 message does not contain parent id"));
            return;
        }

        records.add(new FinalUpsertItem(candidate.stagingId(), payload.businessId(), payload.parentBusinessId(), payload.rawBody()));
    }

    private String normalizeType(String entityType) {
        if (entityType == null) {
            return "";
        }

        return entityType.trim().toUpperCase();
    }

    private record DedupResult(Map<String, FinalUpsertItem> latestByBusinessId, List<Long> obsoleteStagingIds) {
    }
}
