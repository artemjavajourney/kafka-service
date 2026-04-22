# Kafka Consumer Service — Updated Architecture Baseline

## Selected processing model

The service consumes one Kafka topic with multiple partitions.
Business routing is based on message content, not partition number.
Partitioning and ordering reasoning is based on Kafka key.
Ordering is guaranteed only within a single partition.
Global ordering across the topic must not be assumed.

## Updated pipeline

The selected target pipeline is:

Kafka message -> intake validation -> save raw message to `staging_inbox` -> create `event_processing_log` with `STAGED` -> commit/ack offset -> apply batch by `STAGED/DEFERRED` status -> write final tables -> update `event_processing_log`

## Core rule for offset progression

Offset progression is allowed only after successful durable intake persistence:
- raw Kafka message is saved to `staging_inbox`
- initial audit row is saved to `event_processing_log`

Commit/ack before durable intake persistence is an architectural violation.

## Why staging is part of the baseline now

Staging/inbox persistence is the durable intake boundary and allows decoupling Kafka consumption from DB apply throughput.
Final table writes happen in a separate apply phase and can be retried independently from Kafka offset progression.

## Intake phase responsibilities

- consume the whole topic
- read raw Kafka record
- extract minimum technical metadata when possible
- save the raw message and Kafka metadata to staging
- initialize audit status
- return success only after durable persistence

## Apply phase responsibilities

- read staged rows by processing status (`STAGED`/`DEFERRED`)
- route by parsed `entity_type` to per-entity handlers
- call per-entity handlers in the apply phase
- write final business tables
- decide `APPLIED / SKIPPED / FAILED / DEFERRED`
- update audit row

## Audit expectations

`event_processing_log` is lifecycle-based and per Kafka message.
Typical states:
- `STAGED`
- `APPLIED`
- `SKIPPED`
- `FAILED`
- `DEFERRED`

## Extension rule

A new entity type remains primarily a backend extension:
- add apply handler
- add mapping
- add final persistence logic
- extend metrics/logging if needed

A new entity type does not imply Kafka topology redesign by default.
