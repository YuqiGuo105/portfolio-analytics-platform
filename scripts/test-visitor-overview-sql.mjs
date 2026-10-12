import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const { PGlite } = await import(process.env.PGLITE_MODULE || '@electric-sql/pglite');
const db = new PGlite();
try {
  await db.exec(`create schema analytics_private;
    create table analytics_private.behavior_events_raw(event_id text primary key, site_id text, event_time timestamptz, event_name text, session_id text, country text, page_url text, referrer text);
    create table public.behavior_events(event_id text primary key, device_type text, is_bot boolean);
    insert into analytics_private.behavior_events_raw
      select 'e' || i, 'yuqi.site', '2026-10-05 01:00:00+00'::timestamptz + i * interval '1 minute',
        case when i % 3 = 0 then 'read_progress' else 'page_view' end,
        's' || (i / 3), case when i % 2 = 0 then 'IN' else 'US' end,
        'https://yuqi.site/mcp-guide?private=secret#fragment',
        case when i % 2 = 0 then 'https://linkedin.com/feed/?private=secret' else '' end
      from generate_series(1, 150) i;
    insert into public.behavior_events select 'e' || i, 'mobile', case when i % 10 = 0 then null else i % 3 = 0 end from generate_series(1, 150) i;
    insert into analytics_private.behavior_events_raw values('end-excluded', 'yuqi.site', '2026-10-08 00:00:00+00', 'page_view', 'end', 'US', '/', '');
    insert into analytics_private.behavior_events_raw values('other-site', 'other.site', '2026-10-05 01:00:00+00', 'page_view', 'other', 'US', '/', '');`);
  const template = await readFile(new URL('../analytics-aggregator-service/src/main/resources/queries/visitor-overview.sql', import.meta.url), 'utf8');
  async function query(bucket, from, to, extra = '') {
    const where = `from analytics_private.behavior_events_raw r left join public.behavior_events b on b.event_id = r.event_id
      where r.site_id = 'yuqi.site' and r.event_time >= :from and r.event_time < :to ${extra}`;
    const sql = template.replace('/* visitor_filters */', where).replace(/:bucket\b/g, '$1').replace(/:from\b/g, '$2').replace(/:to\b/g, '$3');
    const result = await db.query(sql, [bucket, from, to]);
    return JSON.parse(Object.values(result.rows[0])[0]);
  }
  const all = await query('day', '2026-10-01T00:00:00Z', '2026-10-08T00:00:00Z');
  assert.equal(all.summary.events, 150); assert.equal(all.summary.pageViews, 100);
  assert.equal(all.summary.sessions, 51); assert.equal(all.summary.countries, 2);
  assert.equal(all.summary.unknown, 15);
  assert.equal(all.timeline.length, 7); assert.equal(all.timeline[0].events, 0);
  assert.equal(all.timeline.reduce((s, b) => s + b.events, 0), 150);
  assert.deepEqual(all.rankings.pages, [{ value: '/mcp-guide', count: 100 }]);
  assert.doesNotMatch(JSON.stringify(all), /secret|fragment|other-site|end-excluded/);
  const human = await query('day', all.from, all.to, 'and b.is_bot = false');
  assert.equal(human.summary.bots, 0); assert.equal(human.summary.unknown, 0);
  assert.equal(human.summary.events, 90);
  const hourly = await query('hour', '2026-10-05T01:30:00Z', '2026-10-06T01:30:00Z');
  assert.equal(hourly.timeline.length, 25); assert.equal(hourly.summary.events, 121);
  const empty = await query('day', all.from, all.to, "and r.country = 'XX'");
  assert.equal(empty.summary.events, 0); assert.ok(empty.timeline.every(b => b.events === 0));
  assert.deepEqual(empty.rankings, {});
  console.log('PASS: PostgreSQL aggregate SQL, 150 rows, retention window boundaries, empty buckets, bot states, sessions, redacted paths/referrers and site isolation.');
} finally { await db.close(); }
