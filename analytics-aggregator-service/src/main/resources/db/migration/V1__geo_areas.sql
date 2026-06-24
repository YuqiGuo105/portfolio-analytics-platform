-- ============================================================================
-- analytics-aggregator-service — V1: geo_areas dimension table
--
-- Single source of truth for spatial buckets. The aggregator never invents
-- a geo_area_id; the enrichment service produces them (e.g. METRO:US:UT:SLC)
-- and they MUST exist here before being referenced as an FK in
-- geo_time_rollups. Unseen ids fall through to the GLOBAL row.
--
-- METRO is the smallest bucket — this is a privacy invariant, not just a
-- modelling choice. Do NOT add a CITY row or finer.
-- ============================================================================

create table if not exists public.geo_areas (
    geo_area_id    text primary key,
    geo_level      text not null,
    parent_id      text,
    name           text not null,
    country        text,
    region         text,
    center_lat     double precision,
    center_lng     double precision,
    created_at     timestamptz not null default now(),
    constraint geo_areas_level_chk
        check (geo_level in ('GLOBAL','COUNTRY','REGION','METRO')),
    constraint geo_areas_parent_fk
        foreign key (parent_id) references public.geo_areas(geo_area_id)
);

create index if not exists geo_areas_level_idx on public.geo_areas(geo_level);
create index if not exists geo_areas_country_idx on public.geo_areas(country);
create index if not exists geo_areas_parent_idx on public.geo_areas(parent_id);

-- ── Seed: bare-minimum hierarchy to exercise the rollup path end-to-end. ──
insert into public.geo_areas (geo_area_id, geo_level, parent_id, name, country, region, center_lat, center_lng) values
    ('GLOBAL',          'GLOBAL',  null,                'Global',           null, null, null,   null),
    ('COUNTRY:US',      'COUNTRY', 'GLOBAL',            'United States',    'US', null, 39.83,  -98.58),
    ('REGION:US:UT',    'REGION',  'COUNTRY:US',        'Utah',             'US', 'UT', 39.32,  -111.09),
    ('METRO:US:UT:Salt Lake City',
                        'METRO',   'REGION:US:UT',      'Salt Lake City',   'US', 'UT', 40.7608, -111.8910)
on conflict (geo_area_id) do nothing;
