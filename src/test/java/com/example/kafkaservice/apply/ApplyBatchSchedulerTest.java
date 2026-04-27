package com.example.kafkaservice.apply;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class ApplyBatchSchedulerTest {

    @Test
    void run_shouldStopWhenOrchestratorReturnsZero() {
        ApplyOrchestrator orchestrator = mock(ApplyOrchestrator.class);
        when(orchestrator.applyNextBatch()).thenReturn(5, 0);

        new ApplyBatchScheduler(orchestrator).run();

        verify(orchestrator, times(2)).applyNextBatch();
    }

    @Test
    void run_shouldCapByMaxLoops() {
        ApplyOrchestrator orchestrator = mock(ApplyOrchestrator.class);
        when(orchestrator.applyNextBatch()).thenReturn(1);

        new ApplyBatchScheduler(orchestrator).run();

        verify(orchestrator, times(20)).applyNextBatch();
    }
}
