package com.example.kafkaservice.audit;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EventProcessingLogRecordTest {

    @Test
    void stagedFactory_shouldBuildExpectedStatus() {
        OffsetDateTime now = OffsetDateTime.now();
        var record = EventProcessingLogRecord.staged(1L, "L", "E", now);

        assertEquals(ProcessingLogStatus.STAGED, record.status());
        assertNull(record.errorMessage());
    }

    @Test
    void failedFactory_shouldBuildExpectedStatus() {
        OffsetDateTime now = OffsetDateTime.now();
        var record = EventProcessingLogRecord.failed(2L, "L", "err", now);

        assertEquals(ProcessingLogStatus.FAILED, record.status());
        assertEquals("err", record.errorMessage());
    }
}
