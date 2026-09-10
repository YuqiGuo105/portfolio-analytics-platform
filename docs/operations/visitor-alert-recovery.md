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
  `notification.get_publication_delivery` using `visitor-alert:<incidentId>` to
  verify the downstream recipient delivery state before declaring recovery complete.

Never place internal tokens in committed commands, logs, or screenshots. Do not
disable authentication or modify visitor records to force an alert.
