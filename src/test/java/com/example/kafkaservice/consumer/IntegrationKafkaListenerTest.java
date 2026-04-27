package com.example.kafkaservice.consumer;

import com.example.kafkaservice.apply.ApplyOrchestrator;
import com.example.kafkaservice.intake.IntakeResult;
import com.example.kafkaservice.intake.KafkaIntakeService;
import com.example.kafkaservice.intake.ParseStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class IntegrationKafkaListenerTest {

    @Test
    void listen_shouldDelegateToIntakeService() {
        KafkaIntakeService intakeService = mock(KafkaIntakeService.class);
        when(intakeService.intake(any())).thenReturn(new IntakeResult(1L, "L", "ENTITY_1", ParseStatus.PARSED));
        ApplyOrchestrator orchestrator = mock(ApplyOrchestrator.class);

        IntegrationKafkaListener listener = new IntegrationKafkaListener(intakeService, orchestrator);
        listener.listen(new ConsumerRecord<>("topic", 0, 1L, "k", "v"));

        verify(intakeService).intake(any());
        verifyNoInteractions(orchestrator);
    }
}
