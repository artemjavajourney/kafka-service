package com.example.kafkaservice.intake;

import com.example.kafkaservice.audit.EventProcessingLogRecord;
import com.example.kafkaservice.message.ParsedRawMessage;
import com.example.kafkaservice.message.RawMessageParser;
import com.example.kafkaservice.repository.EventProcessingLogRepository;
import com.example.kafkaservice.repository.StagingInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaIntakeService {

    private final RawMessageParser rawMessageParser;
    private final StagingInboxRepository stagingInboxRepository;
    private final EventProcessingLogRepository eventProcessingLogRepository;

    @Transactional
    public IntakeResult intake(ConsumerRecord<String, String> record) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        ParsedRawMessage parsed = rawMessageParser.parse(record.value(), null);

        StagingInboxRecord stagingRecord = new StagingInboxRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                now
        );

        long stagingId = stagingInboxRepository.insertIfAbsent(stagingRecord);

        eventProcessingLogRepository.insertStagedIfAbsent(
                EventProcessingLogRecord.staged(
                        stagingId,
                        parsed.loadingId(),
                        parsed.entityType(),
                        now
                )
        );

        log.info(
                "Kafka message staged: topic={}, partition={}, offset={}, key={}, loadingId={}, entityType={}, parseStatus={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                parsed.loadingId(),
                parsed.entityType(),
                parsed.parseStatus()
        );

        return new IntakeResult(
                stagingId,
                parsed.loadingId(),
                parsed.entityType(),
                parsed.parseStatus()
        );
    }
}
