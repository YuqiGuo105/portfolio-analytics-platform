-- V16 is already deployed; pin function resolution without replacing bodies or grants.
ALTER FUNCTION analytics_private.reject_governance_audit_mutation()
    SET search_path = pg_catalog, analytics_private;
ALTER FUNCTION analytics_private.capture_visitor_receipt_metadata()
    SET search_path = pg_catalog, analytics_private;
ALTER FUNCTION analytics_private.check_governance_delete()
    SET search_path = pg_catalog, analytics_private;
ALTER FUNCTION analytics_private.check_governance_delete_bound()
    SET search_path = pg_catalog, analytics_private;
ALTER FUNCTION analytics_private.stamp_governance_expiry()
    SET search_path = pg_catalog, analytics_private;
ALTER FUNCTION analytics_private.visitor_evidence_v1(public.behavior_events, TEXT)
    SET search_path = pg_catalog, analytics_private;
ALTER FUNCTION analytics_private.visitor_receipt_capture_v1(analytics_private.visitor_evidence_receipts)
    SET search_path = pg_catalog, analytics_private;
