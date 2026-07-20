-- A committed inbox row is the durable idempotency record for one source event.
-- It is written in the same transaction as facts, sessions and rollups.
create table if not exists public.analytics_kafka_inbox (
    event_id         text primary key,
    consumer_group   text not null,
    kafka_topic      text not null,
    kafka_partition  integer not null,
    kafka_offset     bigint not null,
    processed_at     timestamptz not null default now(),
    constraint analytics_kafka_inbox_offset_unique
        unique (consumer_group, kafka_topic, kafka_partition, kafka_offset)
);

create index if not exists analytics_kafka_inbox_processed_idx
    on public.analytics_kafka_inbox(processed_at);

-- Refresh throttling must share the database transaction. A Redis claim made
-- before commit can survive a process crash and incorrectly suppress replay.
create table if not exists public.analytics_visit_throttle (
    throttle_key text primary key,
    event_id     text not null,
    expires_at   timestamptz not null,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now()
);

create index if not exists analytics_visit_throttle_expiry_idx
    on public.analytics_visit_throttle(expires_at);
