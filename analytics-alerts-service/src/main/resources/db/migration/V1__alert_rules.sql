-- analytics-alerts-service — V1: alert_rules
-- One row = one user-defined threshold rule. The evaluator reads the
-- "enabled = true" subset every cron tick.

create table if not exists public.alert_rules (
    rule_id           bigserial primary key,
    site_id           text not null,
    name              text not null,
    event_type        text not null,                 -- e.g. page_view, click
    geo_level         text not null default 'GLOBAL',
    geo_area_id       text,
    granularity       text not null default '5m',
    threshold         bigint not null,
    comparator        text not null default '>=',    -- '>=' or '<='
    cooldown_seconds  integer not null default 1800,
    enabled           boolean not null default true,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now(),
    constraint alert_rules_granularity_chk check (granularity in ('5m','1d')),
    constraint alert_rules_geo_level_chk   check (geo_level in ('GLOBAL','COUNTRY','REGION','METRO')),
    constraint alert_rules_comparator_chk  check (comparator in ('>=','<=')),
    constraint alert_rules_threshold_chk   check (threshold >= 0)
);

create index if not exists alert_rules_enabled_idx on public.alert_rules(enabled);
create index if not exists alert_rules_site_idx    on public.alert_rules(site_id);
