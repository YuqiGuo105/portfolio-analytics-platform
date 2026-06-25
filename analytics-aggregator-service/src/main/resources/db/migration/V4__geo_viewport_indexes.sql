-- ============================================================================
-- analytics-aggregator-service — V4: indexes for viewport / LOD queries
--
-- The public markers endpoint now takes optional bbox + geo_level filters
-- (see PublicVisitsController#markers). Without these indexes, every pan or
-- zoom-in scans the join and re-evaluates the lat/lng range predicate over
-- the whole geo_areas table.
--
-- Index 1 supports the spatial range scan on geo_areas. Postgres can use a
-- btree on (center_lat, center_lng) for the lat BETWEEN .. plus lng BETWEEN
-- combo via index-only scan — adequate at our data size; we don't need GiST
-- until we have orders of magnitude more rows.
--
-- Index 2 covers the LOD filter: when the globe is at "world" zoom we ask
-- for geo_level='COUNTRY' rollups only, and at "local" we want METRO. The
-- existing geo_time_rollups_lookup_idx in V2 leads on (site_id, granularity,
-- bucket_time, geo_level) which is great for the time-window scan but not
-- for an explicit equality on geo_level when joining by geo_area_id. Adding
-- a (site_id, granularity, geo_level, geo_area_id) index lets the planner
-- pick the cheaper path for "give me all METRO rows joined back to
-- geo_areas".
-- ============================================================================

create index if not exists geo_areas_center_idx
    on public.geo_areas (center_lat, center_lng);

create index if not exists geo_time_rollups_level_area_idx
    on public.geo_time_rollups (site_id, granularity, geo_level, geo_area_id);
