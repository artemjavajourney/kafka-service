package com.example.kafkaservice.apply;

import com.example.kafkaservice.apply.handler.ApplyEntityHandler;
import com.example.kafkaservice.audit.ProcessingLogStatus;
import com.example.kafkaservice.repository.EventProcessingLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ApplyOrchestrator {

    private static final int DEFAULT_BATCH_SIZE = 500;

    private final EventProcessingLogRepository eventProcessingLogRepository;
    private final List<ApplyEntityHandler> handlers;

    @Transactional
    public int applyNextBatch() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        List<ApplyCandidate> candidates = eventProcessingLogRepository.claimNextBatch(DEFAULT_BATCH_SIZE, now);
        if (candidates.isEmpty()) {
            return 0;
        }

        List<ApplyStatusUpdate> statusUpdates = new ArrayList<>();
        Map<String, List<ApplyCandidate>> candidatesByType = new HashMap<>();
        for (ApplyEntityHandler handler : handlers) {
            candidatesByType.put(handler.supportedType(), new ArrayList<>());
        }

        for (ApplyCandidate candidate : candidates) {
            String normalizedType = normalizeType(candidate.entityType());
            List<ApplyCandidate> typedCandidates = candidatesByType.get(normalizedType);
            if (typedCandidates == null) {
                statusUpdates.add(ApplyStatusUpdate.skipped(candidate.stagingId(), "Unknown entity type: " + candidate.entityType()));
                continue;
            }

            if (candidate.body() == null || !candidate.body().isObject()) {
                statusUpdates.add(ApplyStatusUpdate.failed(candidate.stagingId(), "body_json is absent or invalid"));
                continue;
            }

            typedCandidates.add(candidate);
        }

        for (ApplyEntityHandler handler : handlers) {
            handler.handle(candidatesByType.get(handler.supportedType()), statusUpdates);
        }

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

    private String normalizeType(String entityType) {
        if (entityType == null) {
            return "";
        }

        return entityType.trim().toUpperCase();
    }
}
