-- Durable delivery state for retrying incident notifications after transient failures.

alter table public.incidents
  add column if not exists notification_attempts integer not null default 0,
  add column if not exists last_notification_attempt_at timestamptz;

create index if not exists incidents_pending_notification_idx
  on public.incidents(last_notification_attempt_at, created_at)
  where notified = false;
