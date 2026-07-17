# Portfolio Platform Cost Guardrails

## Objective

Keep the complete production platform within USD 30-50 per month. The operating
target is USD 40 and the GCP alert budget is USD 45, leaving a small response
window before the USD 50 ceiling.

## Monthly envelope

| Cost area | Target | Guardrail |
| --- | ---: | --- |
| Warm agent instance | about $10 | One 1 vCPU / 512 MiB request-based instance |
| Agent model usage | up to $22.50 | `$0.75` daily application budget |
| Other Cloud Run traffic | $0-7 | Scale to zero and one maximum instance |
| Artifact Registry and network | $1-3 | Cleanup policy and low traffic allowance |
| Operational reserve | $4-8 | Absorbs cold starts, retries, and price variance |
| Aiven, Supabase, Vercel, Grafana | $0 | Free plans confirmed on 2026-07-17 |

Expected total: about USD 32-42 per month. Kafka, OpenSearch, Valkey, Supabase,
Vercel, and Grafana are confirmed to be on free plans. A future paid database,
search, cache, hosting, or observability plan is outside this envelope and must
be approved before activation.

## Runtime policy

- `portfolio-agent-service` keeps one warm instance for chat latency and never
  scales above one instance.
- All other HTTP services scale to zero and never above one instance.
- Kafka consumers scale to zero and never above one instance. Delivery is
  eventual and cold starts are an accepted tradeoff for this portfolio workload.
- Notification publishing uses the synchronous, idempotent HTTP content-event
  path. It does not require an always-on instance.
- Agent model calls stop admitting new paid work after the configured daily
  application budget is reserved.

Run the audit locally:

```bash
PROJECT_ID=portfolio-notify-prod \
REGION=us-central1 \
./ops/cloud-run-cost-guardrails.sh audit
```

Apply the approved policy after reviewing drift:

```bash
./ops/cloud-run-cost-guardrails.sh apply
```

The scheduled GitHub Actions workflow audits daily. `apply` is manual so an
unexpected service is never modified silently.

## Billing controls

- GCP budget: `portfolio-platform-monthly-45`, scoped to
  `portfolio-notify-prod`, with 50%, 75%, 90%, 100%, and forecast alerts.
- Billing budgets notify; they do not stop resources. Instance limits and the
  application model budget are the actual spend containment controls.
- Review GCP billing by service weekly until two complete billing cycles are
  available. Adjust the agent daily limit first if forecast exceeds USD 45.

## External service requirements

- Aiven Kafka, both configured OpenSearch services, and Valkey are confirmed
  free as of 2026-07-17.
- Supabase, Vercel, and Grafana are confirmed free as of 2026-07-17.
- Treat any plan upgrade or automatic overage setting as a budget policy change.
  Review the projected total before enabling it.
- Free-plan capacity limits are platform constraints, not reasons to enable
  automatic paid overage.

## Artifact cleanup

`ops/artifact-cleanup-policy.json` keeps the five newest images while making
images older than 30 days eligible for deletion. Apply it in dry-run mode first:

```bash
gcloud artifacts repositories set-cleanup-policies portfolio \
  --project=portfolio-notify-prod \
  --location=us-central1 \
  --policy=ops/artifact-cleanup-policy.json \
  --dry-run
```

Review the policy results before removing `--dry-run`.
