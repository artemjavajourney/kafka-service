package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.apply.ApplyStatusUpdate;

import java.util.List;

public interface ApplyEntityHandler {

    String supportedType();

    void handle(List<ApplyCandidate> candidates, List<ApplyStatusUpdate> statusUpdates);
}
