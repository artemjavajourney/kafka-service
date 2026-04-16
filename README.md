# kafka-service-target

Kafka consumer service with durable intake, staging/audit, and dependency-aware apply into final tables.

## What this version implements

This project now implements end-to-end pipeline for two example entities (`ENTITY_1`, `ENTITY_2`):

Kafka -> minimal intake validation -> durable save to `staging_inbox` -> create `event_processing_log` row with `STAGED` -> apply by `loading_id` in dependency order -> write final tables -> update status (`APPLIED` / `DEFERRED` / `FAILED` / `SKIPPED`)

## Why this design is used

All business entities arrive in one topic, and message order can be unsafe for relational dependencies.
To avoid foreign-key violations and data loss, intake is always durable first, and final apply is executed separately by dependency order.

## Main packages

- `config` - configuration properties and application entry point
- `consumer` - thin Kafka listener
- `intake` - intake flow, metadata extraction, statuses, staging model
- `audit` - audit status and audit row model
- `repository` - explicit JDBC repositories for staging, audit, and apply reads/updates
- `apply` - apply orchestrator and business payload extraction
- `finaltable` - repositories writing to final business tables

## Runtime notes

The project is intended to open and compile in an IDE. To run it, you need:
- a reachable Kafka broker
- a PostgreSQL database
- correct credentials in `application.yml`

## Entity contract used by apply phase

Current apply implementation expects JSON where `result_json.body` contains:
- `entity_type` (or `entityType`) with values `ENTITY_1` / `ENTITY_2`
- business identifier in one of: `id`, `entity_id`, `entityId`, `business_id`, `businessId`
- for `ENTITY_2`, parent id in one of: `entity1_id`, `parent_id`, `parentId`, `parent_entity_id`, `parentEntityId`

If parent is missing/not yet in final table, message is marked `DEFERRED` and retried when another message with the same `loading_id` arrives.
