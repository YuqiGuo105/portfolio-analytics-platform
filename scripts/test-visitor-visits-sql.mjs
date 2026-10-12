import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const { PGlite } = await import(process.env.PGLITE_MODULE || '@electric-sql/pglite');
const db = new PGlite();
try {
  await db.exec(`create schema analytics_private;
    create table analytics_private.behavior_events_raw(
      event_id text primary key, site_id text, event_time timestamptz, server_time timestamptz,
      event_name text, session_id text, anon_id text, country text, region text, city text,
      page_url text, target_url text, referrer text, user_agent text, ip_address text,
      latitude double precision, longitude double precision, properties jsonb);
    create table public.behavior_events(event_id text primary key, page_path text, device_type text, browser text, os text, is_bot boolean);`);
  async function insert(id, session = 's1', progress, overrides = {}) {
    const event = { site: 'test.site', name: progress === undefined ? 'page_view' : 'read_progress',
      path: '/', at: '2026-10-12T10:00:00Z', properties: progress === undefined ? {} : { progressPercent: progress }, ...overrides };
    await db.query(`insert into analytics_private.behavior_events_raw
      (event_id,site_id,event_time,event_name,session_id,page_url,ip_address,properties)
      values ($1,$2,$3,$4,$5,$6,'198.51.100.24',$7)`, [id,event.site,event.at,event.name,session,event.path,JSON.stringify(event.properties)]);
    await db.query(`insert into public.behavior_events values ($1,null,'mobile','Safari','iOS',false)`, [id]);
  }
  await insert('opened');
  for (let i = 0; i < 120; i++) await insert(`progress-${i}`, 's1', [25,50,75,100][i % 4], { at: new Date(Date.parse('2026-10-12T10:01:00Z') + i * 1000).toISOString(), path: i % 2 ? '/?utm_source=sample' : 'https://test.site/#read' });
  // The last event need not have the highest progress.
  await insert('latest', 's1', 25, { at: '2026-10-12T11:00:00Z' });
  for (const [i, value] of [true, '', -1, 101, 'invalid', null].entries()) await insert(`invalid-${i}`, 's1', value);
  await insert('another-page', 's1', 75, { path: '/mcp-guide' });
  await insert('same-ip-new-session', 's2', 50);
  await insert('missing-session-a', null, 25);
  await insert('missing-session-b', null, 75);
  await insert('click', 's1', undefined, { name: 'click' });
  await insert('other-site', 's1', 100, { site: 'other.site' });
  await insert('end-excluded', 's1', 100, { at: '2026-10-13T00:00:00Z' });
  const template = await readFile(new URL('../analytics-aggregator-service/src/main/resources/queries/visitor-visits.sql', import.meta.url), 'utf8');
  async function query(page = 0, size = 2, filter = '') {
    const where = `from analytics_private.behavior_events_raw r left join public.behavior_events b on b.event_id = r.event_id
      where r.site_id = 'test.site' and r.event_time >= :from and r.event_time < :to ${filter}`;
    const names = ['from', 'to', 'limit', 'offset', 'page'];
    const sql = template.replace('/* visitor_filters */', where).replace(/:(from|to|limit|offset|page)\b/g, (_, name) => `$${names.indexOf(name) + 1}`);
    const result = await db.query(sql, ['2026-10-12T00:00:00Z','2026-10-13T00:00:00Z',size,page * size,page]);
    return JSON.parse(Object.values(result.rows[0])[0]);
  }
  const first = await query();
  assert.equal(first.page.totalElements, 6); assert.equal(first.page.totalPages, 3);
  assert.equal(first.summary.totalEvents, 133);
  const visit = first.items[0];
  assert.equal(visit.eventId, 'latest'); assert.equal(visit.visit.eventCount, 128);
  assert.equal(visit.visit.maxProgressPercent, 100);
  assert.deepEqual(visit.visit.checkpoints.map(p => p.percent), [25,50,75,100]);
  assert.equal(Date.parse(visit.visit.openedAt), Date.parse('2026-10-12T10:00:00Z'));
  const all = [...first.items, ...(await query(1)).items, ...(await query(2)).items];
  assert.equal(new Set(all.map(e => e.eventId)).size, 6, 'No visit split across pages');
  assert.equal(all.filter(e => !e.visit).length, 3, 'Missing sessions and clicks remain separate');
  assert.equal(all.find(e => e.sessionId === 's2').visit.eventCount, 1, 'IP never becomes identity');
  assert.equal((await query(0, 25, "and r.session_id = 's1'")).page.totalElements, 3);
  const pastEnd = await query(3); assert.deepEqual(pastEnd.items, []); assert.equal(pastEnd.page.totalElements, 6);
  const empty = await query(0, 25, "and r.session_id = 'none'");
  assert.deepEqual(empty.items, []); assert.equal(empty.summary.totalEvents, 0); assert.equal(empty.page.totalPages, 0);
  assert.doesNotMatch(JSON.stringify(all), /other-site|end-excluded/);
  console.log('PASS: PostgreSQL visit grouping before pagination, peak progress, ordered deduplicated checkpoints, malformed values, separate sessions/pages/actions, site/time bounds and empty pages.');
} finally { await db.close(); }
