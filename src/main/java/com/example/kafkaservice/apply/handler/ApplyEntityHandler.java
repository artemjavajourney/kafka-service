package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.apply.support.ResolvedApplyCandidate;

import java.util.List;

public interface ApplyEntityHandler {

    String supportedType();

    void handle(List<ResolvedApplyCandidate> candidates, List<ApplyStatusUpdate> statusUpdates);
}
