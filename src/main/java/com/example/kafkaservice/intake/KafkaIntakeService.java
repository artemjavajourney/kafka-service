package com.example.kafkaservice.intake;

import com.example.kafkaservice.audit.EventProcessingLogRecord;
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

        ParsedMessage parsedMessage = rawMessageParser.parse(record.value());
        String bodyJson = parsedMessage.body() == null ? null : rawMessageParser.toJson(parsedMessage.body());

        StagingInboxRecord stagingRecord = new StagingInboxRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                bodyJson,
                now
        );

        long stagingId = stagingInboxRepository.insertIfAbsent(stagingRecord);

        EventProcessingLogRecord processingLogRecord = parsedMessage.isBodyUsable()
                ? EventProcessingLogRecord.staged(stagingId, parsedMessage.loadingId(), parsedMessage.entityType(), now)
                : EventProcessingLogRecord.failed(stagingId, parsedMessage.loadingId(), parsedMessage.errorMessage(), now);

        eventProcessingLogRepository.insertIfAbsent(processingLogRecord);

        log.info(
                "Kafka message staged: topic={}, partition={}, offset={}, key={}, loadingId={}, entityType={}, parseStatus={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                parsedMessage.loadingId(),
                parsedMessage.entityType(),
                parsedMessage.parseStatus()
        );

        return new IntakeResult(
                stagingId,
                parsedMessage.loadingId(),
                parsedMessage.entityType(),
                parsedMessage.parseStatus()
        );
    }
}
