# Visitor governance (local implementation, disabled by default)

No deployment, production DB access, credentials, provider integration, WORM or
independent timestamp service is part of this change.

## Integration contract

Base: `/api/admin/visitor-governance`. Existing `X-Internal-Token` authentication
applies. The verified Next admin proxy must overwrite (not relay browser input)
`X-Admin-Actor` with its authenticated user.id. Optional actor: absent becomes
`internal-service`; supplied values must match `[A-Za-z0-9_-]{1,64}`. No emails.
The backend trusts the authenticated proxy, not the header alone.

Enable only in a local test environment with `ANALYTICS_GOVERNANCE_ENABLED=true`.
Default is false: no receipt/audit writes, worker activity, or governance data API.
Governance endpoints return 404 when disabled (after existing authentication).
All success responses are HTTP 200, JSON, no `data` wrapper. IDs and dates are
strings; dates are UTC ISO-8601. Responses are `Cache-Control: no-store`.

```ts
type Controls = {
  independentTimestamp: "not_configured";
  storageProtection: "local_database";
  authenticity: "local_integrity_only";
};
type Reason = "incident_review" | "legal_preservation" | "data_quality";
type Batch = {
  batchId: string; createdAt: string; expiresAt: string; receiptCount: number;
  manifestSha256: string; status: "archived" | "expired";
  held: boolean; holdReason: Reason | null; holdExpiresAt: string | null;
};
type Audit = {
  auditId: string; occurredAt: string; actor: string; action: string; target: string;
  batchId: string | null; reason: string | null; affectedCount: number; outcome: string;
};
type Overview = {
  enabled: true;
  policy: {
    policyVersion: "visitor-evidence-v1"; schemaVersion: 1;
    receiptRetentionDays: number; archiveRetentionDays: number;
    auditRetentionDays: 180; tombstoneRetentionDays: 180; holdDays: 90;
    maxBatchSize: 500; defaultBatchSize: 100; scheduledArchiveEnabled: boolean;
    holdScope: "governance_batch_only";
  };
  counts: {received: number; pending: number; archived: number; expired: number; held: number};
  latestBatches: Batch[]; latestAudit: Audit[]; externalControls: Controls;
  maintenance: MaintenanceHealth;
};
type WorkCounts = {
  releasedHolds: number; expiredPendingReceipts: number;
  expiredArchivedReceipts: number; expiredBatches: number;
  purgedReceipts: number; purgedBatches: number; purgedAudit: number;
  archivedReceipts: number;
};
type MaintenanceHealth = {
  lastSuccessAt: string | null; lastFailureAt: string | null;
  status: "never_run" | "completed" | "failed"; intervalSeconds: number;
  lastAffectedCounts: WorkCounts | Record<string, never>;
};
type ArchiveResult = {status: "archived" | "empty"; batch: Batch | null; archivedCount: number};
type Verification = {
  batchId: string; verifiedAt: string; status: "verified" | "tampered" | "expired";
  manifestValid: boolean | null; receiptHashesValid: boolean | null;
  sourceIntegrity: "matched" | "mismatch" | "partial" | "unavailable" | "not_checked";
  sourceCheckedCount: number; sourceMissingCount: number; sourceMismatchCount: number;
};
type Manifest = {
  schemaVersion: 1; policyVersion: "visitor-evidence-v1";
  batchId: string; createdAt: string; expiresAt: string; externalControls: Controls;
  receipts: {receiptId: string; eventRef: string; receivedAt: string; expiresAt: string;
    canonicalEvidence: string; evidenceSha256: string}[];
};
type ExportResult = {batchId: string; manifestSha256: string; canonicalManifest: string; manifest: Manifest};
type HoldResult = {batch: Batch; changed: boolean};
```

| Method / suffix | Request | Response |
| --- | --- | --- |
| GET /overview | none | Overview, latest lists bounded to 20 |
| POST /archive | absent body or `{ "limit": 100 }`, integer 1..500 | ArchiveResult, one synchronous bounded transaction |
| POST /verify | `{ "batchId": "UUID" }` | Verification |
| GET /export | `?batchId=UUID` | ExportResult, JSON attachment |
| POST /hold | `{ "batchId": "UUID", "reason": "incident_review" }` | HoldResult, sets or explicitly renews the 90-day hold |
| DELETE /hold | same JSON body and enum reasons | HoldResult |

Business errors: `{ "error": "code", "message": "safe text" }`.
400: invalid_request, invalid_actor. 404: governance_disabled, batch_not_found.
409: integrity_check_failed, batch_expired. 503: governance_unavailable.
Existing token filter retains its `{ "error": "..." }` 401/503 format.
Unknown JSON properties are rejected, including free-text hold metadata.

`received = pending + archived + expired`; all are receipt counts. `held` is the
subset of archived receipts in held batches. Empty arrays are arrays, nullable
fields are explicit null. Counts describe retained receipt rows, not permanent
lifetime counters. Expired receipt/batch tombstones remain for 180 days after
observed payload expiry; then counts decrease and that receipt's replay-deduplication
window ends. Archive retries create no duplicate retained receipt membership;
a retry can consume the next pending batch, so callers should refresh overview
after an uncertain response rather than assuming request-level idempotency.

## Evidence, integrity and limitations

Only the canonical behavior projection is snapshotted: allowlisted event category,
browser-asserted event time/consent, reported upstream server time, source schema
version and derived bot classification. Arbitrary event names become `other`.
Known `outbound_link_clicked` events are retained. `properties.action` exposes only
github/linkedin/x/twitter/youtube (case-normalized); otherwise platform is null.
Public page paths are restricted to `/`, `/mcp-guide`, `/terminal`, `/works-list`,
`/blogs`, and `/blog-single/<UUID>` or `/work-single/<UUID>`; other paths become
`[other]`. Queries/fragments have already been removed by canonical ingestion.
Only numeric progressPercent checkpoints 0/25/50/75/100 survive; otherwise null.
These fields are browser assertions, not proof that content was read.
`sourceIntegrityProjection` explicitly identifies the source fields being checked.
Site ID is fixed configuration, never accepted from an admin request.
No IP/hash of IP, session/anon identifier, location, full URL, arbitrary path, UA,
arbitrary property value, query text or token is copied to the archive. The internal receipt has a source
event ID solely for indexed integrity comparison, never included in export;
export uses a SHA256 event reference. These are pseudonymous, not anonymous.

Coverage starts at ingestion processed while the feature is enabled. There is no
historical receipt backfill, and old visitor records are not retrospectively
certified. A replay accepted by the existing ingestion path can snapshot an
already-persisted source row; receivedAt means this capture time, not the historical
event's arrival time. Throttled, rejected and inbox-deduplicated events do not get
new receipts. The legacy raw-only persistence entry point is unchanged.

The database receipt clock is server-observed. Event time/type/consent are browser
assertions; upstream server time is producer-reported, not an independent clock.
Derived bot classification is not identity proof. Policy and schema versions are
stored in each evidence payload and manifest.

`canonicalEvidence` is the exact UTF-8 PostgreSQL jsonb text captured at receipt.
Its lowercase SHA256 is `evidenceSha256`. Do not reserialize it before hashing.
`canonicalManifest` is compact JSON with recursively lexicographically sorted
object keys, ordered receipt IDs (numeric ascending), no whitespace, UTF-8.
Hash those exact bytes for `manifestSha256`. Export also includes the parsed
manifest for convenience. Immutable manifest metadata excludes mutable holds.

Verification checks stored manifest bytes, reconstructed membership/metadata,
receipt payload hashes and the same minimized projection of still-present source
rows. Missing source rows are explicitly partial/unavailable, never a match.
An insert-time database trigger also stores a separate capture SHA256 over receipt
ID, configured site, event reference, receivedAt, expiresAt, evidence SHA256 and
policy/schema versions. Archive/verify compare it before trusting capture metadata,
and verify the internal source ID still hashes to eventRef. Changing only a receipt
timestamp, expiry, reference or ID cannot be sealed into a new valid archive.
The capture checksum is internal; the unchanged export manifest binds those fields
for independent verification after archive. Like all local hashes, it is not an
independent signature and a privileged coordinated rewrite can replace it.
This does not authenticate excluded source fields. Coordinated database-owner
rewrites of evidence and hashes remain possible. `local_integrity_only` is not
non-repudiation, WORM, a signature, a trusted timestamp, or proof of human activity.

Holds apply only to a governance batch, not raw facts, visitor_logs, source facts,
Kafka, backups, or other services. Existing raw/source retention is unchanged.
Governance payload retention defaults to 90 days, configurable only within 1..90
while enabled. Archive expiry is the earlier of its configured window and the
earliest member receipt deadline. Expiry clears receipt payloads/source pointers
and manifests, with audit entries. The DB stamps immutable `expired_at` on that
transition; receipt and batch tombstones become purge-eligible 180 days later.
Audit metadata becomes purge-eligible 180 days after `occurred_at`. Batch summaries
are purged only after all their member tombstones have been purged.

Holds record their set time and deadline, at most 90 days per explicit set/renewal.
POST /hold renews to DB server time plus 90 days and audits `renew_hold`; DELETE
releases early. Expired holds no longer authorize evidence export; maintenance
clears them with `expire_hold` audit and expires overdue payloads. An expired hold
cannot resurrect payloads past their retention deadline. Renewals can extend
preservation repeatedly while the hold is active; there is no total lifetime cap.
An elapsed deadline is eligibility, not confirmation of physical deletion.
Backlog, scheduler unavailability or disabling governance can delay cleanup.
No erasure guarantee for backups/WAL or unqualified privacy/compliance claim is made.

Governance service transactions have a 10-second deadline (propagated by Spring
JDBC to statement query timeouts). Overview reads only batch summary columns, not
stored manifests, and limits recent lists to 20. Exact receipt counts still scan
all retained site rows, including tombstones; they are not constant time or
materialized counters. Finite retention limits history under successful maintenance,
not the incoming event rate. Large backlogs can hit the timeout and return 503.

Audit covers this application's existing admin visitor read requests and governance
operations. Read/export audit means request/serialization authorization, not proof
of delivery or a human viewing the response. It is not DB-wide access auditing.
No raw query, URL, free-text reason, token or exception text is recorded. The
application exposes no audit update/delete API; DB triggers reject ordinary audit
UPDATE/TRUNCATE. DELETE requires time eligibility and is capped at 500 rows per
statement. The trusted service role receives no direct DELETE grant, only EXECUTE
on the fixed-target, site-scoped SECURITY DEFINER purge function with a fixed
search_path. Every nonempty function purge appends `purge_receipts`, `purge_batches`
or `purge_audit` with its count in the same transaction. Owner/superuser statements
can bypass application auditing or alter/disable these controls; this is not WORM.
V17 fixes `search_path = pg_catalog, analytics_private` on the seven other
governance functions without changing applied V16, function bodies or privileges.
The purge function already had that setting. PostgreSQL regressions exercise the
V16-to-V17 upgrade, function metadata, and transition-table delete bounds on all
three governance tables, including a same-named caller temporary table.
Local V17 verification on 2026-11-10: the focused command below passed 93 tests
(33 governance PostgreSQL, 10 existing PostgreSQL access, 50 other), with no skips
or failures on PostgreSQL 18.6. Production advisor clearance was not checked here.
For existing Visitors reads the proxy may provide `X-Admin-Action` from the fixed
allowlist `visitor_read`/`visitor_export` (default visitor_read), UUID
`X-Admin-Request-Id` and `X-Admin-Export-Format` csv/json. Valid correlation/format
metadata is stored privately without changing the public Audit response shape.
Each exported page is a separate requested audit entry with the same request ID.
Bad metadata is rejected, never copied verbatim. Audit write failures fail closed.

Existing findings outside this change: raw reads already require an internal
token; raw exact facts are retained separately; existing Flyway startup calls
repair() and disables validate-on-migrate; existing source/legacy retention is
not governed by these holds. None is represented as a new production guarantee.

## Maintenance integration

Configuration keys (defaults shown; release enablement is a separate action):

```text
ANALYTICS_GOVERNANCE_ENABLED=false
ANALYTICS_GOVERNANCE_SCHEDULED_ARCHIVE_ENABLED=false
ANALYTICS_GOVERNANCE_WORKER_INTERVAL_SECONDS=3600
ANALYTICS_GOVERNANCE_RECEIPT_RETENTION_DAYS=90
ANALYTICS_GOVERNANCE_ARCHIVE_RETENTION_DAYS=90
```

The existing token-protected POST `/api/internal/workers/drain?maxWaitMs=2500`
invokes governance synchronously after its existing outbox drain. No new scheduler,
external call, public request or raw-retention invocation was added. The DB stores
last successful cleanup and enforces the configured 3600..86400-second interval;
an advisory transaction lock serializes concurrent maintenance attempts. Feature
off skips all governance DB work and preserves the old drain response.
`maxWaitMs` controls only the pre-existing outbox drain. Governance adds transaction
budgets of 6 seconds for cleanup, 3 seconds for optional archive, and 1 second for
a failure checkpoint, with local SQL/lock deadlines. These are transaction/query
budgets, not a hard end-to-end HTTP/network/connection-acquisition deadline.

Each run reserves at most 100 old audit rows, 100 receipt tombstones, 49 batch
tombstones, 50 expired holds, 200 pending receipts, and 501 archived receipt/batch
rows (at most 10 batches): at most 1000 cleanup target rows plus bounded operation
audit/checkpoint writes. Individual purge calls never exceed 500. Optional archive
handles at most 100 receipts in a **separate transaction after cleanup commits**.
A tampered pending receipt can block that archive, but cannot roll back cleanup
or its audit. The next cleanup remains due on the hourly interval.

The existing drain response keeps `status` and `claimed`; when governance is on
it adds `governance: {status: "completed" | "not_due" | "busy",
maintenance: MaintenanceHealth, affectedCounts: WorkCounts}`. An optional restricted
`target=visitor-governance` skips the unrelated outbox for focused internal tests;
other target values are rejected with 400. Existing internal-token auth applies.
Cleanup failure returns 503 `governance_maintenance_failed`; optional archive failure
returns 503 `governance_archive_failed` after the successful cleanup commit.

Overview's health is initially `never_run`, null timestamps and `{}` counts.
`lastSuccessAt` means successful **cleanup commit**, even if optional archive later
fails. Such an archive failure sets `status=failed`, records `lastFailureAt` and
an archive-rejected audit, preserving cleanup counts and success time. A later
successful due run sets `completed`; the last historical failure time is retained.
Counts describe the last successful cleanup plus its successful optional archive,
not cumulative totals. Failure classifications are bounded internal enums, never
exception text. If the DB itself is unavailable, persisting failure health can
also fail; fixed application log messages are the fallback. Disabling governance
pauses retention and read auditing. A stale health timestamp requires operational
attention; the API does not independently monitor the scheduler.

## Local testing

Tests must use synthetic data only. The PostgreSQL suite accepts only
`jdbc:postgresql://127.0.0.1:PORT/analytics_test` (or localhost), and destroys its
test schemas. Never point it at a shared database. Do not source production env.

```sh
env -u ANALYTICS_TEST_JDBC_URL -u ALERT_TEST_JDBC_URL \
  ANALYTICS_GOVERNANCE_ENABLED=false ./mvnw -o -Pcoverage verify

env ANALYTICS_TEST_JDBC_URL=jdbc:postgresql://127.0.0.1:55439/analytics_test \
  ANALYTICS_TEST_DB_USER=governance_local ANALYTICS_TEST_DB_PASSWORD= \
  ANALYTICS_GOVERNANCE_BENCHMARK=true \
  ./mvnw -o -pl analytics-aggregator-service -am test \
  -Dtest='*Governance*,BehaviorEventAccessPostgresTest,AdminTokenFilterTest,KafkaEventBatchProcessorTest,RetentionServiceTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

### Local verification on 2026-11-10

Full default-off `-Pcoverage verify`: BUILD SUCCESS, 221 passed, zero failures/errors; 50
environment-gated database test entries skipped in that invocation. The aggregator
database tests were executed separately below, not represented as mock coverage;
the unrelated alerts PostgreSQL suite was not run. JaCoCo line coverage:
common 84.44%, aggregator 57.22%, alerts 66.49%. The existing effective POM gate is
50% (its old comments/workflow label say 60%); no gate/exclusion was changed.
Local JDK 25 emitted JaCoCo 0.8.12 unsupported-JDK-class instrumentation warnings;
application classes compile to Java 21 and the checks passed. CI's Java 21 run
remains the release authority; this local result does not establish CI success.

The second command passed 89 tests, including 29 governance PostgreSQL tests and
10 existing PostgreSQL access regressions, with zero failures/errors/skips. This
includes real loopback Tomcat HTTP requests through the actual token filter, MVC,
service transactions and PostgreSQL, as well as real Spring feature-off wiring.
Kafka/Redis/Flyway auto-configuration and application-wide component scanning are
not started by the HTTP harness; the test setup applies and validates Flyway
migrations explicitly. All HTTP servers close at test completion.

Tests include capture-time metadata tampering, content/source tampering, receipt
replay deduplication, transaction rollback, audit-failure rollback of destructive
operations/holds, simultaneous archive workers, bounded archive/retention, and
RLS against nonempty tables with accidental SELECT grants reintroduced.
Retention regressions cover the exact 180-day audit boundary, tombstone purge and
its operation audit, role/target restrictions, 90-day hold renewal/auto-expiry,
hourly durable health/concurrency, fixed cleanup quotas, and a real poison archive
failure after eligible tombstone/payload cleanup with committed audit/health.

Local synthetic timing (two warmups, seven measured samples, PG 18.6 loopback):
receipt append of 100 records median 19.195 ms, max 19.475 ms; archive of 100 median
23.247 ms, max 23.964 ms. Capture measures the additional receipt append with an
already-seeded source, including its standalone local transaction/connection;
it is not full ingestion latency, HTTP latency, a load test or a production promise.

The synthetic export is regenerated by the real backend test at
`analytics-aggregator-service/target/visitor-governance-backend-evidence.json`
(the Maven module's build directory, portable across macOS and Linux). Its manifest SHA256,
parsed-manifest equivalence and each exact canonicalEvidence SHA256 were also
checked independently using Node's built-in crypto module. No Node dependency was
added. Never use this synthetic file as evidence about a real visitor.

Runtime used: PostgreSQL 18.6 ARM64 binary, downloaded into `/private/tmp` from
the public `@embedded-postgres/darwin-arm64` package; SHA512 checked against the
registry metadata. No repo dependency was added. Existing Flyway 10.10 warns that
18.6 is newer than its tested PostgreSQL support (16); migration/validation and
all DB tests nevertheless passed locally. No production-version claim is made.

Local data: `/private/tmp/visitor-governance-pg/data`; server log:
`/private/tmp/visitor-governance-pg/server.log`. This disposable instance uses
loopback-only trust authentication with synthetic data. It is not a production
configuration. Restart/stop, when needed:

```sh
/private/tmp/visitor-governance-pg/package/native/bin/pg_ctl \
  -D /private/tmp/visitor-governance-pg/data \
  -l /private/tmp/visitor-governance-pg/server.log \
  -o '-h 127.0.0.1 -p 55439 -k /private/tmp/visitor-governance-pg' start
/private/tmp/visitor-governance-pg/package/native/bin/pg_ctl \
  -D /private/tmp/visitor-governance-pg/data stop -m fast
```

No backend application server is left running. The isolated PostgreSQL instance
was stopped after verification to free resources; it can be restarted using the
command above. Never run both destructive DB suites concurrently
against the same database. No commit, push, deploy, production connection or
production credential access was performed.
