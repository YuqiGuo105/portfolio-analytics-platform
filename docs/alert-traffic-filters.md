# Visitor Alert Traffic Filters

Alert rules support `filters.bot`: `ALL`, `EXCLUDE`, or `ONLY`. Existing rules
default to `ALL`. `EXCLUDE` counts only aggregator records with `is_bot=false`;
`ONLY` counts `is_bot=true`. Unknown classifications are excluded from both.
This is traffic classification, not proof that a visitor is human.

Use authenticated Admin MCP tools to change a rule, not database updates:

1. Read the existing rule with `alerts.get_rule`.
2. Call `alerts.prepare_change` with `action: "UPDATE"`, its `ruleId`, a reason,
   a stable `_idempotencyKey`, and `patch: {"filters":{"bot":"EXCLUDE"}}`.
3. Review the returned diff. Apply only the approved `changeId` with
   `alerts.apply_change`, `_confirmed: true`, and stable idempotency keys.
4. Read the rule back and call `visitor.explain_match` to verify the stored filter.
   `visitor.rule_test` accepts the same nested filters for a read-only draft test.

Omitting filters preserves the policy on existing rules, including legacy PUT
requests. Explicit `ALL` removes filtering. Invalid filter values or unknown patch
fields fail validation rather than silently counting all traffic.

Scheduled evaluation, single-rule preview, replay, and emailed visit examples use
the same policy. Incidents freeze the rule filter; delivery retries preserve the
first stored summary and idempotency key. Previously queued incidents keep their
original policy, so a rule change does not rewrite historical notifications.

The migration only adds a defaulted policy column. No production rule is changed
by deployment. Alert database evidence remains unavailable to anonymous or ordinary
authenticated database roles.
