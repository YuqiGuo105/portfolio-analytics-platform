# Visitor Alert Recovery

The scheduled evaluator reads five-minute or daily rollups, persists a deduplicated
incident, and sends an idempotent event to the notification service. An accepted
notification event is not proof that an email reached a recipient.

## Regression Tests

CI runs `AlertPostgresTest` against an isolated PostgreSQL 16 service. Local runs
require a disposable database named `alerts_test` on loopback:

```sh
ALERT_TEST_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/alerts_test \
ALERT_TEST_DB_USER=alerts_test \
ALERT_TEST_DB_PASSWORD=local-test-only \
mvn -pl analytics-alerts-service -am test
```

The test fixture truncates its tables and rejects remote/shared database URLs.
Tests exercise JDBC timestamp binding, empty queues, lease reclaim, stale workers,
concurrent claims, retry exhaustion, protected HTTP evaluation, downstream HTTP
failure recovery, and replay deduplication. CI must supply these variables so the
PostgreSQL tests are not skipped.

## Visit Evidence in Email

Alert emails distinguish the event timestamp, server receipt, evaluation window,
and incident creation time. Up to three latest matching canonical events include
their page path, event ID, device/browser and approximate city/region/country.
The window's count remains the rule measurement; samples are not the entire cohort.
No raw IP, coordinates, query strings, or inferred street addresses are included.
Missing or expired records are explicitly labeled unavailable, never replaced by
the current time. The first notification summary is persisted on the incident
before dispatch so retries cannot change its evidence. Direct database access to
incidents is denied to `anon` and `authenticated`; use the protected admin API.

Set `ANALYTICS_ALERT_DISPLAY_ZONE` (also a GitHub Actions repository variable) to
an IANA zone such as `America/Denver`; the default is explicitly labeled `UTC`.
Deploy the notification-service change that preserves `ADMIN_ALERTS` bodies first,
then the alerts service, or the old article preview formatter will truncate details.
Existing delivered emails are immutable and are not automatically resent.

The PostgreSQL HTTP replay test writes a synthetic payload to
`analytics-alerts-service/target/test-artifacts/visitor-alert-payload.json`.
In the notification-service checkout, pass its absolute path to
`mvn test -Dalert.e2e.payload=/absolute/path/visitor-alert-payload.json` to verify
the same payload through recipient fan-out, deduplication and captured MIME
delivery. This test sends no real email and writes an HTML preview under
`target/test-artifacts/visitor-alert-email.html`.

## Recover Missed Alerts

Normal evaluation checks the current and previous bucket. For an outage longer
than that window, an authenticated operator may send a JSON request to
`POST /api/internal/alert-evaluation/replay` with the existing `X-Internal-Token`:

```json
{
  "ruleId": 1,
  "from": "2026-09-03T15:00:00Z",
  "to": "2026-09-10T15:00:00Z"
}
```

- Use a fixed past interval of at most seven days; retain that exact interval on retries.
- Only enabled `>=` rules with a positive threshold are eligible.
- Only the newest matching stored bucket for that rule is considered, preventing
  one outage from generating a backlog of emails. This is a recovery notification,
  not a full per-bucket historical replay.
- Existing incident keys and cooldowns remain enforced. Repeated requests cannot
  recreate the same incident, including after its cooldown expires.
- `matched: true` means historical data met the rule, not that a new incident or
  email was necessarily created. `deliveryVerified` intentionally remains false.
- Check `alerts.list_incidents` for the rule, then
  `notification.get_publication_delivery` using the idempotency key `incident:<incidentId>` to
  verify the downstream recipient delivery state before declaring recovery complete.

Never place internal tokens in committed commands, logs, or screenshots. Do not
disable authentication or modify visitor records to force an alert.
