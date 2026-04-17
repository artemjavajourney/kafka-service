package com.example.kafkaservice.apply.support;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.apply.model.BusinessPayload;

public record ResolvedApplyCandidate(
        ApplyCandidate candidate,
        BusinessPayload payload
) {
}
