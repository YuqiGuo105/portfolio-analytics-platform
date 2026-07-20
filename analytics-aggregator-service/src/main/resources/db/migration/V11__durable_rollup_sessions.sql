-- Redis HLL updates cannot participate in the Postgres projection transaction.
-- This exact contribution ledger makes unique-session deltas rollback-safe.
create table if not exists public.analytics_rollup_sessions (
    site_id       text not null,
    bucket_time   timestamptz not null,
    granularity   text not null,
    geo_level     text not null,
    geo_area_id   text not null,
    event_type    text not null,
    session_hash  text not null,
    created_at    timestamptz not null default now(),
    primary key
        (site_id, bucket_time, granularity, geo_level, geo_area_id, event_type, session_hash)
);

create index if not exists analytics_rollup_sessions_created_idx
    on public.analytics_rollup_sessions(created_at);
