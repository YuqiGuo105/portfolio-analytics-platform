# Visitor Automation Evidence

## Incident

A read-only investigation on 2026-09-12 found crawler traffic that was recorded
as `bot=false`. The affected User-Agents embedded `meta-externalagent/1.1` in
normal-looking Chrome and Edge strings. A whole-word `bot`/`crawl` match missed
these product tokens. A false flag was being mistaken for verified human traffic.

For a fixed seven-day Dallas window ending 2026-09-13 04:10 UTC, the protected
API returned 57 events across 14 sessions: 14 page views, 36 read-progress events,
and seven engaged-time events. All 57 self-identified with that automation token.
Eight sessions had read-progress spans below one second. These are strong
automation indications, not verified operator identity or proof of maliciousness.
The time window is UTC; it ends on September 12 in America/Denver. Session counts
must not be described as a count of people.

## Changes

- Detect automation product tokens, including crawler suffixes and known products
  without a `bot` suffix, even when embedded in browser-like User-Agents.
- Keep unknown browsers unverified; do not classify by geography or IP address.
- Default segment preview to the stored `page_view` event name and normalize case.
- Query only `5m` rollups; adding `1d` rollups double-counts the same events.
- Return preview granularity and state explicitly that bot traffic is included.
- Complement ingestion with the ADMIN MCP edge's per-event evidence and bounded
  page/session summary. Historical flags are preserved and conflicts are visible.

No historical backfill, alert-policy change, traffic blocking, schema change,
external enrichment, or model call is part of this fix. The ingestion correction
applies after deployment; historical corrections need a separately reviewed plan.
Five-minute rollup results describe buckets, not exact event-time boundaries.

## Verification

Aggregator and alert-service tests: 150 passed, zero failures; 20 existing
PostgreSQL container integration tests were skipped because Docker was unavailable.
The local MCP edge and gateway were additionally exercised against the live,
read-only analytics API, including the complete Dallas sample and access-denial
checks. Public and lower-role MCP clients cannot discover or execute visitor
detail tools. This does not claim that a deployment or OAuth-client rollout has
already occurred. Raw IP addresses and session identifiers are omitted here.
