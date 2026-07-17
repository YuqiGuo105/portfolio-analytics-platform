-- Keep bounded admin visitor queries index-backed for the 30-day raw retention window.
CREATE INDEX IF NOT EXISTS idx_behavior_events_raw_site_time
    ON analytics_private.behavior_events_raw (site_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_events_raw_site_event_time
    ON analytics_private.behavior_events_raw (site_id, event_name, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_events_raw_site_session_time
    ON analytics_private.behavior_events_raw (site_id, session_id, event_time DESC)
    WHERE session_id IS NOT NULL;
