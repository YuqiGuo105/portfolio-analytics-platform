-- Session tracking: per-session aggregation and funnel step recording.
-- Allows the dashboard to show session duration, bounce rate, and conversion funnels.

CREATE TABLE IF NOT EXISTS public.sessions (
    session_id   TEXT        NOT NULL,
    site_id      TEXT        NOT NULL,
    anon_id      TEXT,
    first_event  TIMESTAMPTZ NOT NULL,
    last_event   TIMESTAMPTZ NOT NULL,
    page_views   INT         NOT NULL DEFAULT 0,
    clicks       INT         NOT NULL DEFAULT 0,
    duration_ms  BIGINT      NOT NULL DEFAULT 0,
    entry_page   TEXT,
    exit_page    TEXT,
    device_type  TEXT,
    browser      TEXT,
    os           TEXT,
    country      TEXT,
    geo_area_id  TEXT,
    PRIMARY KEY (session_id, site_id)
);

-- Index for time-range queries on the analytics dashboard
CREATE INDEX IF NOT EXISTS idx_sessions_site_time
    ON public.sessions (site_id, first_event DESC);

CREATE TABLE IF NOT EXISTS public.funnel_steps (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id   TEXT        NOT NULL,
    site_id      TEXT        NOT NULL,
    step_name    TEXT        NOT NULL,
    page_url     TEXT,
    event_type   TEXT        NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL,
    step_order   INT         NOT NULL DEFAULT 0
);

-- Index for per-session step ordering
CREATE INDEX IF NOT EXISTS idx_funnel_steps_session
    ON public.funnel_steps (site_id, session_id, step_order);
