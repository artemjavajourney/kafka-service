package com.example.kafkaservice.apply;

import com.example.kafkaservice.apply.handler.ApplyEntityHandler;
import com.example.kafkaservice.audit.ProcessingLogStatus;
import com.example.kafkaservice.repository.EventProcessingLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ApplyOrchestratorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void applyNextBatch_shouldGroupByTypeAndPersistStatuses() throws Exception {
        EventProcessingLogRepository repo = mock(EventProcessingLogRepository.class);

        ApplyEntityHandler h1 = mock(ApplyEntityHandler.class);
        when(h1.supportedType()).thenReturn("ENTITY_1");
        doAnswer(invocation -> {
            List<ApplyCandidate> list = invocation.getArgument(0);
            List<ApplyStatusUpdate> updates = invocation.getArgument(1);
            for (ApplyCandidate c : list) {
                updates.add(ApplyStatusUpdate.inserted(c.stagingId()));
            }
            return null;
        }).when(h1).handle(anyList(), anyList());

        when(repo.claimNextBatch(eq(500), any(OffsetDateTime.class))).thenReturn(List.of(
                new ApplyCandidate(1L, "L1", "entity_1", objectMapper.readTree("{}")),
                new ApplyCandidate(2L, "L2", "unknown", objectMapper.readTree("{}")),
                new ApplyCandidate(3L, "L3", "ENTITY_1", null)
        ));

        ApplyOrchestrator orchestrator = new ApplyOrchestrator(repo, List.of(h1));

        int processed = orchestrator.applyNextBatch();

        assertEquals(3, processed);
        ArgumentCaptor<List<ApplyStatusUpdate>> captor = ArgumentCaptor.forClass(List.class);
        verify(repo).batchUpdateStatuses(captor.capture(), any(OffsetDateTime.class));
        List<ApplyStatusUpdate> updates = captor.getValue();
        assertEquals(3, updates.size());
        assertEquals(1, updates.stream().filter(s -> s.status() == ProcessingLogStatus.APPLIED).count());
        assertEquals(1, updates.stream().filter(s -> s.status() == ProcessingLogStatus.SKIPPED).count());
        assertEquals(1, updates.stream().filter(s -> s.status() == ProcessingLogStatus.FAILED).count());
    }

    @Test
    void applyNextBatch_shouldReturnZero_whenNoCandidates() {
        EventProcessingLogRepository repo = mock(EventProcessingLogRepository.class);
        when(repo.claimNextBatch(eq(500), any(OffsetDateTime.class))).thenReturn(new ArrayList<>());

        ApplyOrchestrator orchestrator = new ApplyOrchestrator(repo, List.of());

        assertEquals(0, orchestrator.applyNextBatch());
        verify(repo, never()).batchUpdateStatuses(anyList(), any());
    }
}
