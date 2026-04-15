# kafka-service-target

Minimal working target version of the Kafka consumer service after the agreed architectural update.

## What this version implements

This project implements **slice 1 only**:

Kafka -> minimal intake validation -> durable save to `staging_inbox` -> create `event_processing_log` row with `STAGED` -> listener returns successfully

What is intentionally **not implemented yet**:
- stage -> final apply orchestrator
- per-entity final handlers
- final business table writes
- final insert/update/skip/deferred decision logic

## Why this is the correct current slice

The service must first establish a durable intake boundary before business apply logic is added:
- consume raw Kafka message
- stage it safely in DB
- create intake audit row
- allow offset progression only after persistence succeeds

## Main packages

- `config` - configuration properties and application entry point
- `consumer` - thin Kafka listener
- `intake` - intake flow, metadata extraction, statuses, staging model
- `audit` - audit status and audit row model
- `repository` - explicit JDBC repositories for staging and audit writes

## Runtime notes

The project is intended to open and compile in an IDE. To run it, you need:
- a reachable Kafka broker
- a PostgreSQL database
- correct credentials in `application.yml`

## Next slice

After this slice, add:
1. `ApplyOrchestrator`
2. staged row selection by `loading_id`
3. per-entity apply handlers
4. final table persistence
5. audit status update to `APPLIED / SKIPPED / FAILED / DEFERRED`
