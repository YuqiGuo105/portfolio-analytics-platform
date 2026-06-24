-- analytics-alerts-service — V2: incidents
-- Append-only audit of what fired, with a UNIQUE dedup_key so the same
-- (rule, area, bucket) only opens one incident even under retries.

create table if not exists public.incidents (
    incident_id       bigserial primary key,
    rule_id           bigint not null references public.alert_rules(rule_id) on delete cascade,
    site_id           text not null,
    geo_area_id       text,
    bucket_time       timestamptz not null,
    granularity       text not null,
    measured_value    bigint not null,
    threshold         bigint not null,
    comparator        text not null,
    dedup_key         text not null,
    notified          boolean not null default false,
    notified_at       timestamptz,
    created_at        timestamptz not null default now(),
    constraint incidents_dedup_key_uq unique (dedup_key)
);

create index if not exists incidents_rule_idx on public.incidents(rule_id, created_at desc);
create index if not exists incidents_site_idx on public.incidents(site_id, created_at desc);
