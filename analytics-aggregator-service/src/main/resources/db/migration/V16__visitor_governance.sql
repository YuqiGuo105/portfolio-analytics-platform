-- Local application evidence only. No claim of WORM, signatures or trusted time.
CREATE TABLE analytics_private.visitor_evidence_batches (
    batch_id UUID PRIMARY KEY,
    site_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    receipt_count INT NOT NULL CHECK (receipt_count BETWEEN 1 AND 500),
    canonical_manifest TEXT,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'archived' CHECK (status IN ('archived', 'expired')),
    held BOOLEAN NOT NULL DEFAULT FALSE,
    hold_reason TEXT CHECK (hold_reason IN ('incident_review', 'legal_preservation', 'data_quality')),
    hold_set_at TIMESTAMPTZ,
    hold_expires_at TIMESTAMPTZ,
    expired_at TIMESTAMPTZ,
    CHECK (held = (hold_reason IS NOT NULL)),
    CHECK ((held AND hold_set_at IS NOT NULL AND hold_expires_at IS NOT NULL
        AND hold_expires_at > hold_set_at AND hold_expires_at <= hold_set_at + interval '90 days')
        OR (NOT held AND hold_set_at IS NULL AND hold_expires_at IS NULL)),
    CHECK ((status = 'expired') = (expired_at IS NOT NULL)),
    CHECK (status <> 'expired' OR (NOT held AND canonical_manifest IS NULL))
);
CREATE INDEX visitor_evidence_batches_recent
    ON analytics_private.visitor_evidence_batches (site_id, created_at DESC, batch_id);
CREATE INDEX visitor_evidence_batches_expiry
    ON analytics_private.visitor_evidence_batches (site_id, expires_at)
    WHERE status = 'archived' AND NOT held;
CREATE INDEX visitor_evidence_batches_hold_expiry
    ON analytics_private.visitor_evidence_batches (site_id, hold_expires_at) WHERE held;
CREATE INDEX visitor_evidence_batches_purge
    ON analytics_private.visitor_evidence_batches (site_id, expired_at) WHERE status = 'expired';

CREATE TABLE analytics_private.visitor_evidence_receipts (
    receipt_id BIGSERIAL PRIMARY KEY,
    site_id TEXT NOT NULL,
    -- Internal lookup only, removed on expiry and never exported.
    source_event_id TEXT,
    event_ref TEXT NOT NULL UNIQUE CHECK (event_ref ~ '^[0-9a-f]{64}$'),
    received_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    canonical_evidence TEXT,
    evidence_sha256 TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    capture_sha256 TEXT NOT NULL CHECK (capture_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'archived', 'expired')),
    expired_at TIMESTAMPTZ,
    batch_id UUID REFERENCES analytics_private.visitor_evidence_batches(batch_id),
    CHECK (status <> 'archived' OR batch_id IS NOT NULL),
    CHECK ((status = 'expired') = (expired_at IS NOT NULL)),
    CHECK (status <> 'expired' OR (canonical_evidence IS NULL AND source_event_id IS NULL))
);
CREATE INDEX visitor_evidence_receipts_pending
    ON analytics_private.visitor_evidence_receipts (site_id, receipt_id) WHERE status = 'pending';
CREATE INDEX visitor_evidence_receipts_expiry
    ON analytics_private.visitor_evidence_receipts (site_id, expires_at) WHERE status = 'pending';
CREATE INDEX visitor_evidence_receipts_batch
    ON analytics_private.visitor_evidence_receipts (batch_id, receipt_id);
CREATE INDEX visitor_evidence_receipts_purge
    ON analytics_private.visitor_evidence_receipts (site_id, expired_at) WHERE status = 'expired';

-- Capture metadata is anchored at insertion, not first sealed during later archive.
-- The digest is deliberately independent of mutable lifecycle/batch membership.
CREATE FUNCTION analytics_private.visitor_receipt_capture_v1(r analytics_private.visitor_evidence_receipts)
RETURNS TEXT LANGUAGE sql STABLE STRICT AS $$
    SELECT encode(sha256(convert_to(jsonb_build_object(
        'schemaVersion', 1, 'policyVersion', 'visitor-evidence-v1',
        'receiptId', r.receipt_id::text, 'siteId', r.site_id, 'eventRef', r.event_ref,
        'receivedAt', to_char(r.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'expiresAt', to_char(r.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'evidenceSha256', r.evidence_sha256
    )::text, 'UTF8')), 'hex')
$$;
CREATE FUNCTION analytics_private.capture_visitor_receipt_metadata()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.capture_sha256 := analytics_private.visitor_receipt_capture_v1(NEW);
    RETURN NEW;
END $$;
CREATE TRIGGER visitor_receipt_capture_metadata
    BEFORE INSERT ON analytics_private.visitor_evidence_receipts
    FOR EACH ROW EXECUTE FUNCTION analytics_private.capture_visitor_receipt_metadata();
REVOKE ALL ON FUNCTION analytics_private.visitor_receipt_capture_v1(analytics_private.visitor_evidence_receipts),
    analytics_private.capture_visitor_receipt_metadata() FROM PUBLIC;

CREATE TABLE analytics_private.visitor_governance_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    site_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    actor VARCHAR(64) NOT NULL CHECK (actor ~ '^[A-Za-z0-9_-]{1,64}$'),
    action TEXT NOT NULL CHECK (action IN ('visitor_read', 'visitor_export', 'overview',
        'archive', 'verify', 'export', 'hold', 'release_hold', 'renew_hold', 'expire_hold',
        'expire_pending', 'expire_batch', 'purge_receipts', 'purge_batches', 'purge_audit')),
    target TEXT NOT NULL CHECK (target IN ('visitors', 'visitor-visits', 'visitor-overview',
        'visitor-intelligence', 'governance')),
    batch_id UUID,
    reason TEXT CHECK (reason IN ('incident_review', 'legal_preservation', 'data_quality')),
    affected_count INT NOT NULL DEFAULT 0 CHECK (affected_count >= 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('requested', 'completed', 'empty', 'verified',
        'tampered', 'expired', 'unchanged', 'rejected')),
    request_id UUID,
    export_format TEXT CHECK (export_format IN ('csv', 'json'))
);
CREATE INDEX visitor_governance_audit_recent
    ON analytics_private.visitor_governance_audit (site_id, audit_id DESC);
CREATE INDEX visitor_governance_audit_request
    ON analytics_private.visitor_governance_audit (request_id) WHERE request_id IS NOT NULL;
CREATE INDEX visitor_governance_audit_purge
    ON analytics_private.visitor_governance_audit (site_id, occurred_at, audit_id);

-- One bounded health checkpoint per configured site, not per visitor or run.
CREATE TABLE analytics_private.visitor_governance_maintenance (
    site_id TEXT PRIMARY KEY,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed')),
    failure_code TEXT CHECK (failure_code IN ('deadline_exceeded', 'database_error', 'maintenance_error', 'archive_rejected', 'archive_error')),
    last_affected_counts JSONB NOT NULL DEFAULT '{}'::jsonb
);
ALTER TABLE analytics_private.visitor_governance_maintenance ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON analytics_private.visitor_governance_maintenance FROM PUBLIC;

CREATE FUNCTION analytics_private.reject_governance_audit_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'governance audit is append only' USING ERRCODE = '42501';
END $$;
CREATE TRIGGER visitor_governance_audit_append_only
    BEFORE UPDATE OR TRUNCATE ON analytics_private.visitor_governance_audit
    FOR EACH STATEMENT EXECUTE FUNCTION analytics_private.reject_governance_audit_mutation();

-- Time eligibility is enforced even for direct ordinary owner DELETE statements.
-- App roles receive no DELETE grant; they can only invoke bounded purge functions.
CREATE FUNCTION analytics_private.check_governance_delete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'visitor_governance_audit' THEN
        IF OLD.occurred_at > statement_timestamp() - interval '180 days' THEN
            RAISE EXCEPTION 'audit retention deadline not reached' USING ERRCODE = '42501';
        END IF;
    ELSE
        IF OLD.status <> 'expired' OR OLD.expired_at IS NULL
            OR OLD.expired_at > statement_timestamp() - interval '180 days' THEN
            RAISE EXCEPTION 'tombstone retention deadline not reached' USING ERRCODE = '42501';
        END IF;
    END IF;
    RETURN OLD;
END $$;
CREATE FUNCTION analytics_private.check_governance_delete_bound()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT count(*) FROM deleted_rows) > 500 THEN
        RAISE EXCEPTION 'governance purge exceeds 500 rows' USING ERRCODE = '22023';
    END IF;
    RETURN NULL;
END $$;
CREATE FUNCTION analytics_private.stamp_governance_expiry()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status <> 'expired' AND NEW.status = 'expired' THEN
        NEW.expired_at := statement_timestamp();
    ELSIF OLD.expired_at IS DISTINCT FROM NEW.expired_at THEN
        RAISE EXCEPTION 'expiry observation is immutable' USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END $$;
DO $$
DECLARE relation TEXT;
BEGIN
    FOREACH relation IN ARRAY ARRAY['visitor_governance_audit', 'visitor_evidence_receipts', 'visitor_evidence_batches'] LOOP
        EXECUTE format('CREATE TRIGGER governance_delete_eligible BEFORE DELETE ON analytics_private.%I FOR EACH ROW EXECUTE FUNCTION analytics_private.check_governance_delete()', relation);
        EXECUTE format('CREATE TRIGGER governance_delete_bounded AFTER DELETE ON analytics_private.%I REFERENCING OLD TABLE AS deleted_rows FOR EACH STATEMENT EXECUTE FUNCTION analytics_private.check_governance_delete_bound()', relation);
    END LOOP;
    FOREACH relation IN ARRAY ARRAY['visitor_evidence_receipts', 'visitor_evidence_batches'] LOOP
        EXECUTE format('CREATE TRIGGER governance_expiry_observed BEFORE UPDATE ON analytics_private.%I FOR EACH ROW EXECUTE FUNCTION analytics_private.stamp_governance_expiry()', relation);
    END LOOP;
END $$;

CREATE FUNCTION analytics_private.purge_visitor_governance(p_site TEXT, p_target TEXT, p_limit INT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, analytics_private
SET lock_timeout = '1s'
AS $$
DECLARE deleted INT;
BEGIN
    IF p_site IS NULL OR p_site !~ '^[A-Za-z0-9._-]{1,100}$'
        OR p_limit IS NULL OR p_limit < 1 OR p_limit > 500 THEN
        RAISE EXCEPTION 'invalid governance purge request' USING ERRCODE = '22023';
    END IF;
    IF p_target = 'audit' THEN
        DELETE FROM analytics_private.visitor_governance_audit WHERE audit_id IN (
            SELECT audit_id FROM analytics_private.visitor_governance_audit
            WHERE site_id = p_site AND occurred_at <= statement_timestamp() - interval '180 days'
            ORDER BY occurred_at, audit_id LIMIT p_limit FOR UPDATE SKIP LOCKED);
    ELSIF p_target = 'receipts' THEN
        DELETE FROM analytics_private.visitor_evidence_receipts WHERE receipt_id IN (
            SELECT receipt_id FROM analytics_private.visitor_evidence_receipts
            WHERE site_id = p_site AND status = 'expired'
                AND expired_at <= statement_timestamp() - interval '180 days'
                AND canonical_evidence IS NULL AND source_event_id IS NULL
            ORDER BY expired_at, receipt_id LIMIT p_limit FOR UPDATE SKIP LOCKED);
    ELSIF p_target = 'batches' THEN
        DELETE FROM analytics_private.visitor_evidence_batches WHERE batch_id IN (
            SELECT b.batch_id FROM analytics_private.visitor_evidence_batches b
            WHERE b.site_id = p_site AND b.status = 'expired' AND NOT b.held
                AND b.expired_at <= statement_timestamp() - interval '180 days'
                AND b.canonical_manifest IS NULL
                AND NOT EXISTS (SELECT 1 FROM analytics_private.visitor_evidence_receipts r WHERE r.batch_id = b.batch_id)
            ORDER BY b.expired_at, b.batch_id LIMIT p_limit FOR UPDATE OF b SKIP LOCKED);
    ELSE
        RAISE EXCEPTION 'invalid governance purge target' USING ERRCODE = '22023';
    END IF;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    IF deleted > 0 THEN
        INSERT INTO analytics_private.visitor_governance_audit
            (site_id, actor, action, target, affected_count, outcome)
        VALUES (p_site, 'internal-service',
            CASE p_target WHEN 'audit' THEN 'purge_audit' WHEN 'receipts' THEN 'purge_receipts' ELSE 'purge_batches' END,
            'governance', deleted, 'completed');
    END IF;
    RETURN deleted;
END $$;
REVOKE ALL ON FUNCTION analytics_private.check_governance_delete(), analytics_private.check_governance_delete_bound(),
    analytics_private.stamp_governance_expiry(), analytics_private.purge_visitor_governance(TEXT, TEXT, INT) FROM PUBLIC;

-- One versioned projection serves receipt capture and later source comparison.
-- Never serialize the whole source row; all arbitrary text is excluded/allowlisted.
CREATE FUNCTION analytics_private.visitor_evidence_v1(e public.behavior_events, configured_site TEXT)
RETURNS TEXT LANGUAGE sql STABLE STRICT AS $$
    SELECT jsonb_build_object(
        'schemaVersion', 1, 'policyVersion', 'visitor-evidence-v1',
        'siteId', configured_site,
        'eventRef', encode(sha256(convert_to(e.event_id, 'UTF8')), 'hex'),
        'sourceSchemaVersion', e.schema_version,
        'provenance', jsonb_build_object(
            'receivedAt', 'server_observed', 'siteId', 'server_configured',
            'eventRef', 'producer_identifier_digest', 'sourceSchemaVersion', 'producer_reported',
            'eventType', 'browser_asserted', 'eventTime', 'browser_asserted',
            'pagePath', 'browser_asserted', 'progressPercent', 'browser_asserted', 'platform', 'browser_asserted',
            'consentState', 'browser_asserted', 'upstreamServerTime', 'producer_reported',
            'botClassification', 'server_derived'),
        'sourceIntegrityProjection', jsonb_build_object(
            'eventType', CASE WHEN e.event_name IN ('page_view', 'click', 'content_open',
                'read_progress', 'subscribe_started', 'subscribe_verified',
                'recommendation_impression', 'recommendation_click', 'outbound_link_clicked') THEN e.event_name ELSE 'other' END,
            'eventTime', to_char(e.event_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'upstreamServerTime', to_char(e.server_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'consentState', CASE WHEN e.consent_state IN ('granted', 'denied') THEN e.consent_state ELSE 'unknown' END,
            'botClassification', e.is_bot,
            'platform', CASE WHEN e.event_name = 'outbound_link_clicked'
                AND lower(e.properties ->> 'action') IN ('github', 'linkedin', 'x', 'twitter', 'youtube')
                THEN lower(e.properties ->> 'action') ELSE NULL END,
            'pagePath', CASE
                WHEN e.page_path IN ('/', '/mcp-guide', '/terminal', '/works-list', '/blogs') THEN e.page_path
                WHEN e.page_path ~ '^/(blog-single|work-single)/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                    THEN lower(e.page_path)
                ELSE '[other]' END,
            'progressPercent', CASE WHEN jsonb_typeof(e.properties -> 'progressPercent') = 'number'
                AND e.properties -> 'progressPercent' IN ('0'::jsonb, '25'::jsonb, '50'::jsonb, '75'::jsonb, '100'::jsonb)
                THEN (e.properties ->> 'progressPercent')::numeric::int ELSE NULL END
        )
    )::text
$$;

REVOKE ALL ON FUNCTION analytics_private.visitor_evidence_v1(public.behavior_events, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION analytics_private.reject_governance_audit_mutation() FROM PUBLIC;

-- No browser policies; deny by default even if a table grant is reintroduced.
ALTER TABLE analytics_private.visitor_evidence_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE analytics_private.visitor_evidence_batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE analytics_private.visitor_governance_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON analytics_private.visitor_evidence_receipts,
    analytics_private.visitor_evidence_batches, analytics_private.visitor_governance_audit FROM PUBLIC;
REVOKE ALL ON SEQUENCE analytics_private.visitor_evidence_receipts_receipt_id_seq,
    analytics_private.visitor_governance_audit_audit_id_seq FROM PUBLIC;

DO $$
DECLARE reader TEXT;
BEGIN
    FOREACH reader IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader) THEN
            EXECUTE format('REVOKE ALL ON SCHEMA analytics_private FROM %I', reader);
            EXECUTE format('REVOKE ALL ON analytics_private.visitor_evidence_receipts, analytics_private.visitor_evidence_batches, analytics_private.visitor_governance_audit FROM %I', reader);
            EXECUTE format('REVOKE ALL ON SEQUENCE analytics_private.visitor_evidence_receipts_receipt_id_seq, analytics_private.visitor_governance_audit_audit_id_seq FROM %I', reader);
            EXECUTE format('REVOKE ALL ON FUNCTION analytics_private.visitor_evidence_v1(public.behavior_events, TEXT), analytics_private.reject_governance_audit_mutation() FROM %I', reader);
            EXECUTE format('REVOKE ALL ON FUNCTION analytics_private.visitor_receipt_capture_v1(analytics_private.visitor_evidence_receipts), analytics_private.capture_visitor_receipt_metadata() FROM %I', reader);
            EXECUTE format('REVOKE ALL ON analytics_private.visitor_governance_maintenance FROM %I', reader);
            EXECUTE format('REVOKE ALL ON FUNCTION analytics_private.check_governance_delete(), analytics_private.check_governance_delete_bound(), analytics_private.stamp_governance_expiry(), analytics_private.purge_visitor_governance(TEXT, TEXT, INT) FROM %I', reader);
        END IF;
    END LOOP;
    -- Match the existing trusted backend role convention; never grant audit mutation.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        GRANT USAGE ON SCHEMA analytics_private TO service_role;
        GRANT SELECT, INSERT, UPDATE ON analytics_private.visitor_evidence_receipts,
            analytics_private.visitor_evidence_batches TO service_role;
        REVOKE ALL ON analytics_private.visitor_governance_audit FROM service_role;
        GRANT SELECT, INSERT ON analytics_private.visitor_governance_audit TO service_role;
        GRANT USAGE, SELECT ON SEQUENCE analytics_private.visitor_evidence_receipts_receipt_id_seq,
            analytics_private.visitor_governance_audit_audit_id_seq TO service_role;
        GRANT EXECUTE ON FUNCTION analytics_private.visitor_evidence_v1(public.behavior_events, TEXT) TO service_role;
        GRANT EXECUTE ON FUNCTION analytics_private.visitor_receipt_capture_v1(analytics_private.visitor_evidence_receipts)
            TO service_role;
        GRANT SELECT, INSERT, UPDATE ON analytics_private.visitor_governance_maintenance TO service_role;
        GRANT EXECUTE ON FUNCTION analytics_private.purge_visitor_governance(TEXT, TEXT, INT) TO service_role;
    END IF;
END $$;
