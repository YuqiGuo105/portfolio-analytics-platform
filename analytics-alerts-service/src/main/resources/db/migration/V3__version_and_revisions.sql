-- analytics-alerts-service — V3: optimistic locking + revision audit
-- Adds version column for optimistic concurrency control,
-- and a revisions table for full audit trail of rule changes.

ALTER TABLE public.alert_rules
  ADD COLUMN IF NOT EXISTS version integer NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS public.alert_rule_revisions (
    revision_id   bigserial PRIMARY KEY,
    rule_id       bigint NOT NULL REFERENCES public.alert_rules(rule_id) ON DELETE SET NULL,
    version       integer NOT NULL,
    action        text NOT NULL,  -- CREATE, UPDATE, SET_ENABLED
    actor         text,
    reason        text,
    request_id    text,
    before_state  jsonb,
    after_state   jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_rule_revisions_rule_idx
  ON public.alert_rule_revisions(rule_id, created_at DESC);
CREATE INDEX IF NOT EXISTS alert_rule_revisions_actor_idx
  ON public.alert_rule_revisions(actor, created_at DESC);
