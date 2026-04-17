package com.example.kafkaservice.apply;

public record ResolvedApplyCandidate(
        ApplyCandidate candidate,
        BusinessPayload payload
) {
}
