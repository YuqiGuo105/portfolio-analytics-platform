-- Explicit, recoverable notification delivery lifecycle for multi-instance workers.

alter table public.incidents
  add column if not exists notification_state text not null default 'PENDING',
  add column if not exists notification_lease_until timestamptz,
  add column if not exists next_notification_attempt_at timestamptz not null default now(),
  add column if not exists last_notification_error text;

update public.incidents
set notification_state = case when notified then 'DELIVERED' else 'PENDING' end
where notification_state = 'PENDING';

alter table public.incidents
  drop constraint if exists incidents_notification_state_chk;

alter table public.incidents
  add constraint incidents_notification_state_chk check (
    notification_state in ('PENDING', 'DELIVERING', 'RETRY_WAIT', 'DELIVERED', 'DEAD_LETTER')
  );

drop index if exists public.incidents_pending_notification_idx;

create index if not exists incidents_delivery_claim_idx
  on public.incidents(notification_state, next_notification_attempt_at, notification_lease_until, created_at)
  where notified = false;
