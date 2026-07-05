# portfolio-analytics-platform

Visitor analytics, geo aggregation, and alerting for [yuqi.site](https://yuqi.site).
Powers the **rotating globe** on the homepage dashboard and the
**`/analytics`** drill-down page.

Built to run end-to-end on **free-tier** infrastructure:

| Concern | Provider | Free-tier ceiling |
|---|---|---|
| Event bus | Kafka | **2 topics**, 2 partitions each |
| Dedup cache | Redis | 30 MB, SSL |
| Source-of-truth + rollups | Supabase Postgres | 500 MB, 60 conns |
| Service hosting | Render.com | 750 hrs/mo, cold-start after 15 min idle |
| Notifications fan-out | Existing `portfolio-notification-service` | shared |

> Why "platform" and not just "service": this repo replaces the ad-hoc
> `visitor_logs` / `visitor_clicks` tracking that lived directly in the
> Portfolio's Supabase. Those rows are still the source of truth — see
> [§ Backfill](#5-backfill-historical-data) for how they get replayed
> through the new pipeline.

---

## 1. Architecture

### 1.1 System-component diagram (UML / C4-style)

```mermaid
flowchart LR
    %% =================== external actors / clients ===================
    visitor(["Browser visitor"]):::actor
    admin(["Operator (admin CRUD)"]):::actor

    %% =================== Portfolio frontend (Vercel) ===================
    subgraph PORTFOLIO["Portfolio · Next.js on Vercel"]
        direction TB
        dash["DashboardPanels<br/>globe + KPIs + 'More' button"]:::ui
        analyticsPage["/analytics page<br/>globe + drill-down"]:::ui
        ingest["/api/track<br/>Ingestion endpoint"]:::api
        proxy["/api/analytics/[...path]<br/>same-origin read proxy"]:::api
    end

    %% =================== managed services ===================
    subgraph MANAGED["Managed services"]
        direction TB
        kraw[("Kafka topic<br/>analytics.raw.events<br/>2 partitions")]:::topic
        kdlq[("Kafka topic<br/>analytics.events.dlq<br/>2 partitions")]:::topic
        redis[("Redis<br/>event_id SETNX dedup")]:::cache
    end

    %% =================== this repo (Render) ===================
    subgraph PLATFORM["portfolio-analytics-platform · Render"]
        direction TB

        subgraph AGG["service: analytics-aggregator-service :8093"]
            direction TB
            consumer["RawEventConsumer<br/>@KafkaListener<br/>DONE / DLQ / RETRY"]:::comp
            pipeline["EnrichmentPipeline<br/>parse · UA · bot · HMAC ip_hash · GeoSnap to METRO"]:::comp
            dedupSvc["DedupService<br/>SETNX guard"]:::comp
            rollup["RollupUpsertService<br/>5m + 1d UPSERT"]:::comp
            backfill["BackfillService + Runner<br/>visitor_logs / clicks → pipeline → upsert"]:::comp
            publicApi["PublicVisitsController<br/>GET /api/public/visits/*"]:::comp
            dlqProd["DlqProducer"]:::comp
        end

        subgraph ALERTS["service: analytics-alerts-service :8094"]
            direction TB
            adminApi["AlertRuleController<br/>X-Internal-Token gated CRUD"]:::comp
            evaluator["AlertEvaluator<br/>@Scheduled · 1 min"]:::comp
            sender["NotificationSender<br/>HTTP POST"]:::comp
        end
    end

    %% =================== persistence (Supabase) ===================
    subgraph SUPABASE["Supabase Postgres (free)"]
        direction TB
        geoAreas[("geo_areas<br/>dimension")]:::db
        rollups[("geo_time_rollups<br/>5m + 1d fact")]:::db
        alertTbl[("alert_rules · incidents")]:::db
        legacyLogs[("visitor_logs · visitor_clicks<br/>legacy truth of result")]:::db
    end

    %% =================== downstream (existing) ===================
    notif(["portfolio-notification-service<br/>POST /api/content-events"]):::ext

    %% ─────────── live ingest path ───────────
    visitor -- "page_view / click (beacon)" --> ingest
    ingest -- "produce JSON RawEvent" --> kraw
    kraw --> consumer
    consumer -- "parse + enrich" --> pipeline
    pipeline -. "SETNX event_id (fail-open)" .-> dedupSvc
    dedupSvc <--> redis
    pipeline -- "EnrichedEvent" --> rollup
    rollup -- "INSERT ... ON CONFLICT" --> rollups
    consumer -. "parse/missing field" .-> dlqProd
    dlqProd --> kdlq

    %% ─────────── backfill path ───────────
    backfill -- "SELECT *" --> legacyLogs
    backfill -- "synthesize RawEvent<br/>eventId = vl:id / vc:id" --> pipeline

    %% ─────────── read path (globe + /analytics) ───────────
    publicApi -- "JOIN geo_areas" --> rollups
    publicApi -- "lat/lng lookup" --> geoAreas
    proxy -. "GET /api/public/visits/*" .-> publicApi
    analyticsPage --> proxy
    dash -- "'More →' link" --> analyticsPage
    visitor --> dash

    %% ─────────── alerts path ───────────
    admin -- "CRUD rules + token" --> adminApi
    adminApi --> alertTbl
    evaluator -- "scan rollups vs rules" --> rollups
    evaluator -- "open / close incidents" --> alertTbl
    evaluator --> sender
    sender -- "POST + X-Internal-Token" --> notif

    classDef actor fill:#fef3c7,stroke:#b45309,color:#7c2d12
    classDef ui fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a
    classDef api fill:#e0e7ff,stroke:#4338ca,color:#312e81
    classDef comp fill:#ecfdf5,stroke:#047857,color:#064e3b
    classDef topic fill:#fce7f3,stroke:#be185d,color:#831843
    classDef cache fill:#fef9c3,stroke:#a16207,color:#713f12
    classDef db fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e
    classDef ext fill:#f1f5f9,stroke:#475569,color:#1e293b,stroke-dasharray: 4 3
```

> **Read this as a UML component diagram:** rounded boxes are actors,
> stadium / cylinder shapes are external stores (Kafka topics, Redis,
> Postgres tables), and rectangles inside the service subgraphs are
> Spring components. Solid arrows are synchronous calls or Kafka
> produce/consume; dashed arrows are best-effort side-channels
> (dedup / DLQ / read proxy).

### 1.2 ASCII overview

```
            Ingestion API                            Kafka
   (Portfolio Next.js /api/track)   ───→    analytics.raw.events
                                                     │
                                                     ▼
                                       ┌──────────────────────────────┐
                                       │ analytics-aggregator-service │
                                       │  ┌──────────────────────┐    │
   Redis  ◀──────────  SETNX ─────────│  │  EnrichmentPipeline  │    │
   (event_id dedup)                    │  │  UA · bot · ip_hash  │    │
                                       │  │  GeoSnap → METRO     │    │
                                       │  └──────────┬───────────┘    │
                                       │             ▼                │
                                       │  ┌──────────────────────┐    │
                                       │  │ RollupUpsertService  │ ──→│──→ Supabase Postgres
                                       │  │ (5m + 1d granularity)│    │     geo_time_rollups
                                       │  └──────────────────────┘    │
                                       │                              │
                                       │  GET /api/public/visits/*    │
                                       │  → globe + /analytics page   │
                                       └──────────────────────────────┘
                                                     │
                                                     ▼
                                            analytics.events.dlq
                                            (poison-pill records)
                                                     │
                                                     ▼
                                       ┌──────────────────────────────┐
                                       │   analytics-alerts-service   │
                                       │  rule eval @ 1 min cadence   │
                                       │  HTTP → notification-service │
                                       └──────────────────────────────┘
```

### Modules

| Module | Port | Purpose |
|---|---|---|
| `analytics-common` | — | DTOs (`RawEvent`, `EnrichedEvent`, `GeoHint`), topic names, DLQ helper, `Outcome` enum (DONE / DLQ / RETRY) |
| `analytics-aggregator-service` | 8093 | Single Kafka consumer that owns the full **raw → enrich → rollup** pipeline; also runs the backfill `CommandLineRunner` and exposes `/api/public/visits/*` for the globe |
| `analytics-alerts-service` | 8094 | CRUD `/api/admin/alert-rules` (gated by `X-Internal-Token`), `@Scheduled` evaluator, HTTP fan-out to `portfolio-notification-service` |

### Why only 2 topics

The free Kafka plan caps at 2 topics. We previously had a
`raw → enrich service → enriched topic → aggregator` chain requiring 3.
**Enrichment now runs in-process inside the aggregator**, reducing the
wire surface to exactly:

- `analytics.raw.events` — every event from the Ingestion API
- `analytics.events.dlq` — poison-pill records

### Privacy invariants enforced in code

- **No raw IP survives enrichment.** `EnrichedEvent` literally has no
  `ipRaw` field; only `ipHash = HMAC-SHA-256(salt, ipRaw)`.
- **METRO is the smallest spatial bucket.** No city / postcode / IP-block
  data is persisted anywhere.
- **Lat/lng exposed to the dashboard always come from
  `geo_areas.center_lat / center_lng`**, never from a visitor row.
- Salt rotation (`ANALYTICS_HMAC_SALT`) instantly invalidates all
  historical `ip_hash` joins — the intended privacy escape hatch.

---

## 2. Local development

### Prerequisites

- JDK 21 (the Maven wrapper takes care of Maven itself)
- A reachable Postgres (Supabase URL works fine) + Redis (`brew install redis`)
- Optional: a Kafka cluster if you want to exercise the consumer

### Build + test

```bash
./mvnw -B -ntp -Pcoverage verify
```

The `coverage` profile enforces **≥ 60 % line coverage** per module. All
60 unit / slice tests should pass.

### Run the aggregator locally

```bash
cp .env.example .env
# Fill in DB credentials at minimum; Kafka/Redis can be skipped if
# you only want the public read endpoints + backfill mode.

# Steady-state consumer mode
./mvnw -B -ntp -pl analytics-aggregator-service spring-boot:run

# Or build + run the jar
./mvnw -B -ntp -pl analytics-aggregator-service -am -DskipTests package
java -jar analytics-aggregator-service/target/analytics-aggregator-service.jar
```

### Docker Compose

```bash
docker compose up --build
```

Brings up the aggregator (`:8093`) and alerts (`:8094`). Kafka / Redis /
Postgres are still external — drive via `.env`.

---

## 3. Public read API

All endpoints are unauthenticated and CORS-open by design; they only
return the same aggregate data the public dashboard already shows.

### `GET /api/public/visits/markers?days=30`

```json
[
  {
    "geoAreaId": "COUNTRY:US",
    "geoLevel":  "COUNTRY",
    "country":   "US",
    "name":      "United States",
    "region":    null,
    "lat":       39.83,
    "lng":      -98.58,
    "count":     1234
  }
]
```

Joined with `geo_areas.center_lat / center_lng` so the globe can place
pins. METRO buckets win over their COUNTRY parents when seeded.

### `GET /api/public/visits/summary?days=30`

```json
{
  "siteId":    "yuqi.site",
  "days":      30,
  "totals":       { "events": 1234, "clicks": 123, "pageViews": 1111 },
  "topCountries": [ { "country": "US", "count": 800 } ],
  "topDevices":   [ { "deviceType": "desktop", "count": 900 } ],
  "timeSeries":   [ { "bucketTime": "2025-01-01T00:00:00Z", "count": 40 } ]
}
```

Feeds the `/analytics` page on the Portfolio dashboard.

---

## 4. Wiring the Portfolio frontend

In the **`Portfolio`** repo:

1. `pages/analytics.js` renders the drill-down page (globe + KPIs + bars).
2. `pages/api/analytics/[...path].js` is a same-origin proxy that
   forwards `/api/analytics/visits/*` to the upstream aggregator, hiding
   the Render URL and avoiding cross-origin CORS.
3. The dashboard's `<RotatingGlobe>` now has a **More →** overlay button
   linking to `/analytics`.

Configure the proxy with one Vercel env var:

```bash
ANALYTICS_API_URL=https://portfolio-analytics-aggregator.onrender.com
```

(Leave it unset for local development — defaults to `http://localhost:8093`.)

---

## 5. Backfill historical data

The pre-existing `visitor_logs` (~4400 rows) and `visitor_clicks` (~400
rows) on Supabase are the **truth of result** — they predate this
platform. Rather than copy them straight into `geo_time_rollups`, the
aggregator **replays each row through the same enrichment + UPSERT
pipeline** a live Kafka event would take. That way the rollup numbers
match exactly what the live stream would produce.

```bash
# One-off pod / Render deploy with the flag flipped on
ANALYTICS_BACKFILL_ENABLED=true \
ANALYTICS_CONSUMER_ENABLED=false \
java -jar analytics-aggregator-service/target/analytics-aggregator-service.jar
```

The runner is idempotent on the input rows (it gives every synthesized
event a stable id — `vl:<id>` for `visitor_logs`, `vc:<id>` for
`visitor_clicks`), but the rollup UPSERTs themselves are additive — if
you replay twice you'll double the counts. **Truncate `geo_time_rollups`
first if you need a fresh backfill.**

---

## 6. Deploying to Render + Supabase

### 6.1 One-time provisioning

| Step | What | Where |
|---|---|---|
| 1 | Create a **Kafka** service | Cloud console → Services |
| 2 | Create the 2 topics (`analytics.raw.events`, `analytics.events.dlq`) at 2 partitions each | Kafka service → Topics |
| 3 | Create a SASL user, copy the Project CA | Kafka service → Users / CA |
| 4 | Create a **Redis** service | Cloud console → Services |
| 5 | Apply the Flyway migrations to Supabase (any V1–V3 SQL file) | Supabase SQL editor, or let the service do it on first boot |
| 6 | Note the JDBC URL + user + password from Supabase | Supabase → Project Settings → Database |

### 6.2 Apply the Render blueprint

```bash
# From the repo root
render blueprint launch
# …or paste render.yaml into Render Dashboard → Blueprints → New
```

Render will provision two web services from `render.yaml` and prompt for
every env var marked `sync: false` (Kafka creds, Redis URL, Supabase
JDBC URL/user/password, notification token, etc.). The HMAC salt is
auto-generated by Render — copy it out into your password manager since
rotating it invalidates historical `ip_hash` joins.

### 6.3 First-time backfill

Once the aggregator service is up:

1. In Render Dashboard → Service → Environment, set
   `ANALYTICS_BACKFILL_ENABLED=true` and
   `ANALYTICS_CONSUMER_ENABLED=false`.
2. Trigger a manual deploy. The boot log will stream
   `{"event":"backfill_progress","rows":1000}` updates.
3. When you see `backfill_complete`, flip the env vars back
   (`ANALYTICS_BACKFILL_ENABLED=false`, `ANALYTICS_CONSUMER_ENABLED=true`)
   and redeploy. The Kafka consumer will resume from the last committed
   offset.

### 6.4 Smoke-test

```bash
scripts/e2e-smoke.sh https://portfolio-analytics-aggregator.onrender.com
```

Checks `/actuator/health`, `/api/public/visits/markers`, and
`/api/public/visits/summary` and asserts the wire shape consumed by
the Portfolio `/analytics` page. Exits non-zero on the first failure.

---

## 7. Module conventions inherited from `portfolio-notification-service`

- **DONE / DLQ / RETRY** 3-state outcome on every `@KafkaListener`. RETRY
  sleeps 2 s before unacked re-poll; DLQ acks immediately so we don't
  spin on poison pills.
- Flyway runs with `repair()` before `migrate()` (manual hot-fixes don't
  block boot).
- `JdbcTemplate`-driven UPSERTs everywhere; no JPA, no Liquibase.
- Internal admin endpoints are gated by `X-Internal-Token`; everything
  else returns 404. The token is generated by Render on first apply.
- Tests run on **JDK 25**; the `src/test/resources/mockito-extensions/`
  file selects the subclass mock-maker because the default inline-mock
  maker doesn't work on JDK 25 yet.

---

## 8. Repo layout

```
.
├── analytics-common/                 # DTOs + topic names + DlqProducer
├── analytics-aggregator-service/     # Kafka consumer + backfill + read API
│   └── src/main/java/site/yuqi/analytics/aggregator/
│       ├── backfill/                 # BackfillService + Runner
│       ├── config/                   # AggregatorBeansConfig (Flyway, Jackson, IpHash, DLQ)
│       ├── enrich/                   # EnrichmentPipeline + IpHash / UA / Bot / GeoSnap / Dedup
│       ├── kafka/                    # RawEventConsumer (DONE / DLQ / RETRY)
│       ├── service/                  # RollupUpsertService
│       └── web/                      # PublicVisitsController
├── analytics-alerts-service/         # Rule eval + incidents + notification fan-out
├── docker-compose.yml                # Local 2-service stack
├── render.yaml                       # Production blueprint
└── scripts/e2e-smoke.sh              # Post-deploy smoke test
```
