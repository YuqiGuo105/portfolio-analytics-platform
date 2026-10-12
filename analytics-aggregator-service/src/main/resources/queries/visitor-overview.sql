with filtered as materialized (
    select r.event_time, r.event_name, nullif(r.session_id, '') as session_id,
           nullif(upper(trim(r.country)), '') as country,
           nullif(lower(trim(b.device_type)), '') as device,
           b.is_bot,
           coalesce(nullif(split_part(split_part(regexp_replace(coalesce(r.page_url, ''),
               '^https?://[^/]+', '', 'i'), '?', 1), '#', 1), ''), '/') as path,
           case when coalesce(r.referrer, '') = '' then 'Direct'
                when r.referrer ~* '^https?://[^/]+' then lower(substring(r.referrer from '(?i)^https?://([^/?#:]+)'))
                else null end as referrer
    /* visitor_filters */
), buckets as (
    select generate_series(
        date_trunc(:bucket, cast(:from as timestamptz) at time zone 'UTC'),
        date_trunc(:bucket, (cast(:to as timestamptz) - interval '1 microsecond') at time zone 'UTC'),
        case when :bucket = 'hour' then interval '1 hour' else interval '1 day' end
    ) as bucket
), series as (
    select date_trunc(:bucket, event_time at time zone 'UTC') as bucket,
           count(*) as events, count(*) filter (where event_name = 'page_view') as page_views,
           count(distinct session_id) as sessions
    from filtered group by 1
), dimensions as (
    select d.dimension, d.value, count(*) as count
    from filtered f
    cross join lateral (values
        ('countries', f.country), ('devices', f.device), ('referrers', f.referrer),
        ('pages', case when f.event_name = 'page_view' then f.path end)
    ) d(dimension, value)
    where d.dimension <> 'pages' or f.event_name = 'page_view'
    group by d.dimension, d.value
), ranked as (
    select *, row_number() over (partition by dimension order by count desc, value nulls last) as position
    from dimensions
)
select jsonb_build_object(
    'from', to_char(cast(:from as timestamptz) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'to', to_char(cast(:to as timestamptz) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'bucket', :bucket, 'timezone', 'UTC',
    'summary', (select jsonb_build_object(
        'events', count(*), 'pageViews', count(*) filter (where event_name = 'page_view'),
        'sessions', count(distinct session_id), 'countries', count(distinct country),
        'likelyPeople', count(*) filter (where is_bot = false),
        'bots', count(*) filter (where is_bot = true),
        'unknown', count(*) filter (where is_bot is null)
    ) from filtered),
    'timeline', (select coalesce(jsonb_agg(jsonb_build_object(
        'time', to_char(b.bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        'events', coalesce(s.events, 0), 'pageViews', coalesce(s.page_views, 0),
        'sessions', coalesce(s.sessions, 0)
    ) order by b.bucket), '[]'::jsonb) from buckets b left join series s using (bucket)),
    'rankings', (select coalesce(jsonb_object_agg(d.dimension, d.items), '{}'::jsonb) from (
        select dimension, jsonb_agg(jsonb_build_object('value', value, 'count', count)
            order by position) as items from ranked where position <= 5 group by dimension
    ) d)
)::text
