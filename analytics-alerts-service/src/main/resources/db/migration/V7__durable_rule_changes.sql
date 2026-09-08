create table if not exists alert_rule_changes (
    change_id varchar(64) primary key,
    pending_json text not null,
    expires_at timestamptz not null,
    idempotency_key varchar(200) unique,
    response_json text,
    created_at timestamptz not null default now(),
    applied_at timestamptz
);
alter table alert_rule_changes enable row level security;
revoke all on alert_rule_changes from anon, authenticated;

-- The existing FK uses ON DELETE SET NULL; permit that action while retaining revisions.
alter table alert_rule_revisions alter column rule_id drop not null;
