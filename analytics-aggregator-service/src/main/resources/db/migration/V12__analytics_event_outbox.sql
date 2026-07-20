-- Durable producer-side outbox for visitor events written by the first-party
-- tracking endpoint. The browser request commits visitor_logs first; Kafka can
-- be unavailable without losing the event. A relay publishes this payload and
-- marks it SENT only after the broker acknowledges it.
create table if not exists public.analytics_event_outbox (
    id              bigserial primary key,
    event_id        text not null unique,
    partition_key   text not null,
    payload         jsonb not null,
    status          text not null default 'PENDING'
                    check (status in ('PENDING', 'PROCESSING', 'SENT', 'FAILED')),
    attempt_count   integer not null default 0,
    next_attempt_at timestamptz not null default now(),
    lease_until     timestamptz,
    last_error      text,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    sent_at         timestamptz
);

create index if not exists analytics_event_outbox_ready_idx
    on public.analytics_event_outbox(status, next_attempt_at, created_at);

revoke all on public.analytics_event_outbox from public;
revoke all on sequence public.analytics_event_outbox_id_seq from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        revoke all on public.analytics_event_outbox from anon;
        revoke all on sequence public.analytics_event_outbox_id_seq from anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        revoke all on public.analytics_event_outbox from authenticated;
        revoke all on sequence public.analytics_event_outbox_id_seq from authenticated;
    end if;
end $$;

create or replace function public.enqueue_visitor_log_event()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
    stable_event_id text;
begin
    -- Kafka-consumed rows already carry an event id. Skipping them prevents
    -- the compatibility sink from publishing the same event recursively.
    if new.event_id is not null then
        return new;
    end if;

    stable_event_id := 'vl:' || new.id::text;

    update public.visitor_logs
       set event_id = stable_event_id
     where id = new.id
       and event_id is null;

    insert into public.analytics_event_outbox(event_id, partition_key, payload)
    values (
        stable_event_id,
        'yuqi.site',
        jsonb_strip_nulls(jsonb_build_object(
            'eventId', stable_event_id,
            'siteId', 'yuqi.site',
            'eventType', coalesce(nullif(new.event, ''), 'page_view'),
            'eventTime', coalesce(new.local_time, new.created_at, now()),
            'serverTime', coalesce(new.created_at, now()),
            'uaRaw', nullif(new.ua, ''),
            'ipRaw', nullif(new.ip, ''),
            'geoHint', jsonb_strip_nulls(jsonb_build_object(
                'country', nullif(new.country, ''),
                'region', nullif(new.region, ''),
                'city', nullif(new.city, ''),
                'lat', new.latitude,
                'lng', new.longitude,
                'source', 'first-party-track'
            )),
            'schemaVersion', 1,
            'consentState', 'unknown',
            'properties', '{}'::jsonb
        ))
    )
    on conflict (event_id) do nothing;

    return new;
end;
$$;

drop trigger if exists visitor_logs_event_outbox_trigger on public.visitor_logs;
create trigger visitor_logs_event_outbox_trigger
after insert on public.visitor_logs
for each row execute function public.enqueue_visitor_log_event();

