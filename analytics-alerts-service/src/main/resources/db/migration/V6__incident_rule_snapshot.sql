alter table incidents
    add column if not exists rule_version integer not null default 1;

alter table incidents
    add column if not exists rule_snapshot jsonb not null default '{}'::jsonb;
