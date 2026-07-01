-- ============================================================================
-- analytics-aggregator-service — V6: visitor_logs.event_id (idempotency key)
--
-- Context: the Portfolio /api/track endpoint now writes raw events to Kafka
-- as the primary path; the aggregator's RawEventConsumer batch-inserts them
-- into public.visitor_logs (previously written per-request by the API).
--
-- Because Kafka delivery is at-least-once and the consumer only ack's after
-- a successful batch INSERT + rollup upsert, a redelivered batch would
-- create duplicate visitor_logs rows without a natural unique key.
-- The RawEvent carries a UUIDv7 eventId that we plumb into a new nullable
-- event_id column so the batch insert can use ON CONFLICT (event_id) DO NOTHING.
--
-- The column is NULLABLE + the unique index is PARTIAL (event_id IS NOT NULL)
-- so pre-existing rows with a null event_id do not collide with each other,
-- and rows still writable by hand (backfill / migration) don't need an id.
-- ============================================================================

alter table public.visitor_logs
    add column if not exists event_id text;

-- Partial unique index — only enforces uniqueness for rows that actually
-- carry an event_id, so legacy rows keep coexisting.
create unique index if not exists visitor_logs_event_id_uniq_idx
    on public.visitor_logs(event_id)
    where event_id is not null;
