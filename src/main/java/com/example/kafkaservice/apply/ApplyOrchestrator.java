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
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ApplyOrchestrator {

    private final EventProcessingLogRepository eventProcessingLogRepository;
    private final BusinessPayloadExtractor businessPayloadExtractor;
    private final EntityOneRepository entityOneRepository;
    private final EntityTwoRepository entityTwoRepository;

    @Transactional
    public void applyByLoadingId(String loadingId) {
        if (loadingId == null || loadingId.isBlank()) {
            return;
        }

        List<ApplyCandidate> candidates = eventProcessingLogRepository.findForApplyByLoadingId(loadingId);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        for (ApplyCandidate candidate : candidates) {
            try {
                applySingle(candidate, now);
            } catch (Exception e) {
                log.warn(
                        "Apply failed for stagingId={}, loadingId={}, reason={}",
                        candidate.stagingId(),
                        loadingId,
                        e.getMessage()
                );
                eventProcessingLogRepository.updateStatus(
                        candidate.stagingId(),
                        ProcessingLogStatus.FAILED,
                        e.getMessage(),
                        now
                );
            }
        }
    }

    private void applySingle(ApplyCandidate candidate, OffsetDateTime now) {
        BusinessPayload payload = businessPayloadExtractor.extract(candidate.rawMessage(), candidate.entityType());
        String normalizedType = normalizeType(payload.entityType());

        switch (normalizedType) {
            case "ENTITY_1" -> applyEntityOne(candidate, payload, now);
            case "ENTITY_2" -> applyEntityTwo(candidate, payload, now);
            default -> eventProcessingLogRepository.updateStatus(
                    candidate.stagingId(),
                    ProcessingLogStatus.SKIPPED,
                    "Unknown entity type: " + payload.entityType(),
                    now
            );
        }
    }

    private void applyEntityOne(ApplyCandidate candidate, BusinessPayload payload, OffsetDateTime now) {
        if (payload.businessId() == null || payload.businessId().isBlank()) {
            eventProcessingLogRepository.updateStatus(
                    candidate.stagingId(),
                    ProcessingLogStatus.FAILED,
                    "ENTITY_1 message does not contain business id",
                    now
            );
            return;
        }

        entityOneRepository.upsert(payload.businessId(), payload.rawBody());
        eventProcessingLogRepository.updateStatus(candidate.stagingId(), ProcessingLogStatus.APPLIED, null, now);
    }

    private void applyEntityTwo(ApplyCandidate candidate, BusinessPayload payload, OffsetDateTime now) {
        if (payload.businessId() == null || payload.businessId().isBlank()) {
            eventProcessingLogRepository.updateStatus(
                    candidate.stagingId(),
                    ProcessingLogStatus.FAILED,
                    "ENTITY_2 message does not contain business id",
                    now
            );
            return;
        }

        if (payload.parentBusinessId() == null || payload.parentBusinessId().isBlank()) {
            eventProcessingLogRepository.updateStatus(
                    candidate.stagingId(),
                    ProcessingLogStatus.DEFERRED,
                    "ENTITY_2 message does not contain parent id",
                    now
            );
            return;
        }

        if (!entityOneRepository.exists(payload.parentBusinessId())) {
            eventProcessingLogRepository.updateStatus(
                    candidate.stagingId(),
                    ProcessingLogStatus.DEFERRED,
                    "Parent ENTITY_1 not found yet for id=" + payload.parentBusinessId(),
                    now
            );
            return;
        }

        entityTwoRepository.upsert(payload.businessId(), payload.parentBusinessId(), payload.rawBody());
        eventProcessingLogRepository.updateStatus(candidate.stagingId(), ProcessingLogStatus.APPLIED, null, now);
    }

    private String normalizeType(String entityType) {
        if (entityType == null) {
            return "";
        }

        return entityType.trim().toUpperCase();
    }
}
