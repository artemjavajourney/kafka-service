package com.example.kafkaservice.apply;

import java.util.List;

public interface ApplyEntityHandler {

    String supportedType();

    void handle(List<ResolvedApplyCandidate> candidates, List<ApplyStatusUpdate> statusUpdates);
}
