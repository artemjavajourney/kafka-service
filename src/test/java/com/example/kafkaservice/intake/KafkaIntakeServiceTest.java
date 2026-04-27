package com.example.kafkaservice.intake;

import com.example.kafkaservice.audit.EventProcessingLogRecord;
import com.example.kafkaservice.repository.EventProcessingLogRepository;
import com.example.kafkaservice.repository.StagingInboxRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaIntakeServiceTest {

    @Test
    void intake_shouldCreateStagedRecord_whenBodyUsable() {
        RawMessageParser parser = mock(RawMessageParser.class);
        StagingInboxRepository stagingRepo = mock(StagingInboxRepository.class);
        EventProcessingLogRepository logRepo = mock(EventProcessingLogRepository.class);

        var parsed = new ParsedMessage("L1", "ENTITY_1", mock(com.fasterxml.jackson.databind.JsonNode.class), ParseStatus.PARSED, null);
        when(parsed.body().isObject()).thenReturn(true);
        when(parser.parse(any())).thenReturn(parsed);
        when(parser.toJson(any())).thenReturn("{}");
        when(stagingRepo.insertIfAbsent(any())).thenReturn(100L);

        KafkaIntakeService service = new KafkaIntakeService(parser, stagingRepo, logRepo);
        var record = new ConsumerRecord<>("topic", 0, 10L, "k", "{}");

        IntakeResult result = service.intake(record);

        assertEquals(100L, result.stagingId());
        assertEquals(ParseStatus.PARSED, result.parseStatus());

        ArgumentCaptor<EventProcessingLogRecord> captor = ArgumentCaptor.forClass(EventProcessingLogRecord.class);
        verify(logRepo).insertIfAbsent(captor.capture());
        assertEquals("ENTITY_1", captor.getValue().entityType());
    }

    @Test
    void intake_shouldCreateFailedRecord_whenBodyNotUsable() {
        RawMessageParser parser = mock(RawMessageParser.class);
        StagingInboxRepository stagingRepo = mock(StagingInboxRepository.class);
        EventProcessingLogRepository logRepo = mock(EventProcessingLogRepository.class);

        var parsed = new ParsedMessage("L2", null, null, ParseStatus.RAW_ONLY, "bad");
        when(parser.parse(any())).thenReturn(parsed);
        when(stagingRepo.insertIfAbsent(any())).thenReturn(101L);

        KafkaIntakeService service = new KafkaIntakeService(parser, stagingRepo, logRepo);
        var record = new ConsumerRecord<>("topic", 1, 11L, "k", "{}");

        IntakeResult result = service.intake(record);

        assertEquals(101L, result.stagingId());
        assertNull(result.entityType());
        verify(logRepo).insertIfAbsent(any(EventProcessingLogRecord.class));
    }
}
