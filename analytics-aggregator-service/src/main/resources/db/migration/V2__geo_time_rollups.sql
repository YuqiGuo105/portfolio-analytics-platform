-- ============================================================================
-- analytics-aggregator-service — V2: geo_time_rollups
--
-- Single fact table. Both 5m and 1d granularities live here, distinguished
-- by `granularity` so the API can query either with the same SQL shape.
-- The (site_id, bucket_time, granularity, geo_level, geo_area_id,
-- event_type, device_type, browser, os, is_bot, country) tuple is the
-- natural key — that is the upsert ON CONFLICT target.
--
-- We deliberately avoid storing unique-session HLL state in Postgres
-- (it's expensive on the cheap plan); the visit / click counts plus an
-- approximate unique_sessions int (best-effort, may double-count near
-- bucket boundaries) is good enough for the dashboard surface.
-- ============================================================================

create table if not exists public.geo_time_rollups (
    site_id          text not null,
    bucket_time      timestamptz not null,
    granularity      text not null,
    geo_level        text not null,
    geo_area_id      text not null,
    event_type       text not null,
    device_type      text not null default 'unknown',
    browser          text not null default 'unknown',
    os               text not null default 'unknown',
    is_bot           boolean not null default false,
    country          text not null default '',
    event_count      bigint not null default 0,
    unique_sessions  bigint not null default 0,
    updated_at       timestamptz not null default now(),
    constraint geo_time_rollups_granularity_chk
        check (granularity in ('5m','1d')),
    constraint geo_time_rollups_level_chk
        check (geo_level in ('GLOBAL','COUNTRY','REGION','METRO')),
    constraint geo_time_rollups_pk
        primary key (site_id, bucket_time, granularity, geo_level, geo_area_id,
                     event_type, device_type, browser, os, is_bot, country)
);

create index if not exists geo_time_rollups_lookup_idx
    on public.geo_time_rollups(site_id, granularity, bucket_time desc, geo_level);
create index if not exists geo_time_rollups_country_idx
    on public.geo_time_rollups(site_id, granularity, country, bucket_time desc);
create index if not exists geo_time_rollups_area_idx
    on public.geo_time_rollups(site_id, granularity, geo_area_id, bucket_time desc);
