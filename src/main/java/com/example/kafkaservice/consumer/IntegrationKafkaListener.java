package com.example.kafkaservice.consumer;

import com.example.kafkaservice.apply.ApplyOrchestrator;
import com.example.kafkaservice.intake.IntakeResult;
import com.example.kafkaservice.intake.KafkaIntakeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegrationKafkaListener {

    private final KafkaIntakeService kafkaIntakeService;
    private final ApplyOrchestrator applyOrchestrator;

    @KafkaListener(topics = "${app.kafka.topic}")
    public void listen(ConsumerRecord<String, String> record) {
        IntakeResult result = kafkaIntakeService.intake(record);
        applyOrchestrator.applyByLoadingId(result.loadingId());
    }
}
