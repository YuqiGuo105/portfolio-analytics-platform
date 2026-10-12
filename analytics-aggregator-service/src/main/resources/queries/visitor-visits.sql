with filtered as materialized (
    select r.*, b.device_type, b.browser, b.os, b.is_bot,
           case when r.event_name in ('page_view', 'read_progress', 'engaged_time')
                     and nullif(r.session_id, '') is not null and nullif(r.page_url, '') is not null
                then jsonb_build_array('visit', r.site_id, r.session_id,
                     coalesce(nullif(b.page_path, ''), nullif(split_part(split_part(
                         regexp_replace(r.page_url, '^https?://[^/]+', '', 'i'), '?', 1), '#', 1), ''), '/'))::text
                else jsonb_build_array('event', r.event_id)::text end as visit_key,
           case when r.event_name = 'read_progress'
                     and r.properties->>'progressPercent' ~ '^[0-9]{1,3}(\.[0-9]{1,3})?$'
                then case when (r.properties->>'progressPercent')::numeric <= 100
                          then round((r.properties->>'progressPercent')::numeric)::int end end as progress
    /* visitor_filters */
), grouped as (
    select visit_key, count(*) as event_count, min(event_time) as first_event,
           max(event_time) as last_event, max(progress) as max_progress,
           min(event_time) filter (where event_name = 'page_view') as opened_at
    from filtered group by visit_key
), selected as (
    select * from grouped order by last_event desc, visit_key limit :limit offset :offset
), checkpoints as (
    select f.visit_key, f.progress, min(f.event_time) as reached_at
    from filtered f join selected s using (visit_key)
    where f.progress is not null group by f.visit_key, f.progress
), representatives as (
    select distinct on (f.visit_key) f.* from filtered f join selected s using (visit_key)
    order by f.visit_key, f.event_time desc, f.event_id desc
), items as (
    select s.last_event, s.visit_key, jsonb_build_object(
        'eventId', r.event_id, 'siteId', r.site_id, 'eventName', r.event_name,
        'eventTime', r.event_time, 'serverTime', r.server_time,
        'sessionId', r.session_id, 'anonymousId', r.anon_id,
        'pageUrl', r.page_url, 'targetUrl', r.target_url, 'referrer', r.referrer,
        'userAgent', r.user_agent, 'ipAddress', r.ip_address,
        'country', r.country, 'region', r.region, 'city', r.city,
        'latitude', r.latitude, 'longitude', r.longitude,
        'deviceType', r.device_type, 'browser', r.browser, 'os', r.os,
        'bot', r.is_bot, 'properties', coalesce(r.properties, '{}'::jsonb),
        'visit', case when r.visit_key::jsonb->>0 = 'visit' then jsonb_build_object(
            'eventCount', s.event_count, 'firstEventTime', s.first_event,
            'lastEventTime', s.last_event, 'maxProgressPercent', s.max_progress,
            'openedAt', s.opened_at, 'checkpoints', coalesce((
                select jsonb_agg(jsonb_build_object('percent', c.progress, 'eventTime', c.reached_at)
                                 order by c.reached_at, c.progress)
                from checkpoints c where c.visit_key = s.visit_key), '[]'::jsonb)
        ) end) as item
    from selected s join representatives r using (visit_key)
)
select jsonb_build_object(
    'view', 'visits',
    'items', coalesce((select jsonb_agg(item order by last_event desc, visit_key) from items), '[]'::jsonb),
    'summary', (select jsonb_build_object('totalEvents', count(*),
        'uniqueVisitors', count(distinct coalesce(nullif(session_id, ''), nullif(anon_id, ''), nullif(ip_address, ''))),
        'countries', count(distinct nullif(country, '')), 'cities', count(distinct nullif(city, ''))) from filtered),
    'page', jsonb_build_object('number', cast(:page as integer), 'size', cast(:limit as integer), 'totalElements', (select count(*) from grouped),
        'totalPages', ceil((select count(*) from grouped)::numeric / :limit)),
    'from', cast(:from as timestamptz), 'to', cast(:to as timestamptz)
)::text;
