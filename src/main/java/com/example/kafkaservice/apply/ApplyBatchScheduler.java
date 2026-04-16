package com.example.kafkaservice.apply;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ApplyBatchScheduler {

    private final ApplyOrchestrator applyOrchestrator;

    @Scheduled(fixedDelayString = "${app.apply.fixed-delay-ms:2000}")
    public void run() {
        int maxLoops = 20;

        for (int i = 0; i < maxLoops; i++) {
            int processed = applyOrchestrator.applyNextBatch();
            if (processed == 0) {
                return;
            }
        }

        log.info("Apply scheduler reached max loops for a single tick");
    }
}
