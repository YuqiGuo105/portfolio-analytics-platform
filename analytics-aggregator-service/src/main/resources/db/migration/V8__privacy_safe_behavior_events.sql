-- Restricted short-lived raw tier for internal analysis and incident response.
-- No public controller or browser client may query this schema.
CREATE SCHEMA IF NOT EXISTS analytics_private;
REVOKE ALL ON SCHEMA analytics_private FROM PUBLIC;

CREATE TABLE IF NOT EXISTS analytics_private.behavior_events_raw (
    event_id        TEXT        PRIMARY KEY,
    schema_version  INT         NOT NULL DEFAULT 1,
    site_id         TEXT        NOT NULL,
    event_name      TEXT        NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    server_time     TIMESTAMPTZ NOT NULL,
    session_id      TEXT,
    anon_id         TEXT,
    page_url        TEXT,
    target_url      TEXT,
    referrer        TEXT,
    user_agent      TEXT,
    ip_address      TEXT,
    country         TEXT,
    region          TEXT,
    city            TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    properties      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
REVOKE ALL ON analytics_private.behavior_events_raw FROM PUBLIC;
CREATE INDEX IF NOT EXISTS idx_behavior_events_raw_created
    ON analytics_private.behavior_events_raw (created_at);

-- Legacy browser-side reads must not bypass the aggregate API. Supabase roles
-- are revoked conditionally so the migration also works on plain PostgreSQL.
REVOKE SELECT ON public.visitor_logs FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        EXECUTE 'REVOKE SELECT ON public.visitor_logs FROM anon';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        EXECUTE 'REVOKE SELECT ON public.visitor_logs FROM authenticated';
    END IF;
END $$;

-- Canonical serving/feature fact table. Raw IPs, exact coordinates, full
-- referrer URLs and raw user-agent strings are deliberately absent here.
CREATE TABLE IF NOT EXISTS public.behavior_events (
    event_id        TEXT        PRIMARY KEY,
    schema_version  INT         NOT NULL DEFAULT 1,
    site_id         TEXT        NOT NULL,
    event_name      TEXT        NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    server_time     TIMESTAMPTZ NOT NULL,
    session_id      TEXT,
    anon_id_hash    TEXT,
    consent_state   TEXT        NOT NULL DEFAULT 'unknown',
    page_path       TEXT,
    target_path     TEXT,
    referrer_domain TEXT,
    device_type     TEXT,
    browser         TEXT,
    os              TEXT,
    is_bot          BOOLEAN     NOT NULL DEFAULT FALSE,
    ip_hash         TEXT,
    country         TEXT,
    region          TEXT,
    geo_area_id     TEXT,
    properties      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT behavior_events_consent_check
        CHECK (consent_state IN ('granted', 'denied', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_behavior_events_site_time
    ON public.behavior_events (site_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_events_session_time
    ON public.behavior_events (site_id, session_id, event_time)
    WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_behavior_events_name_time
    ON public.behavior_events (site_id, event_name, event_time DESC);

-- Make funnel writes replay-safe and allow event-time ordering across batches.
ALTER TABLE public.funnel_steps ADD COLUMN IF NOT EXISTS event_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_funnel_steps_event_id
    ON public.funnel_steps (event_id) WHERE event_id IS NOT NULL;
