# kafka-service-target

Kafka consumer service with durable intake and scheduled batch apply.

## Current architecture

The pipeline is split into two fully independent stages:

1. **Intake** (Kafka listener)
   - reads Kafka messages
   - performs only minimal metadata extraction
   - stores raw payload to `staging_inbox`
   - creates `event_processing_log` row with `STAGED`

2. **Apply** (scheduler/batch)
   - periodically claims next batch of `STAGED` / `DEFERRED` rows
   - marks them as `PROCESSING`
   - applies business logic in bulk to final tables
   - updates status to `APPLIED`, `SKIPPED`, `DEFERRED`, or `FAILED`

## Why this architecture

It avoids an expensive DB apply attempt on every incoming Kafka message and scales better for high-throughput topics.

## Main packages

- `config` - application boot and scheduling enablement
- `consumer` - Kafka listener (intake only)
- `intake` - intake flow and metadata extraction
- `apply` - batch scheduler and apply orchestration
- `audit` - processing statuses and audit model
- `repository` - staging/audit JDBC access
- `finaltable` - bulk persistence into final tables

## Runtime notes

To run the service, you need:
- Kafka broker
- PostgreSQL
- correct `application.yml`

## Key tunables

- `app.apply.fixed-delay-ms` - scheduler delay between apply ticks
