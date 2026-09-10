-- Canonical events still contain linkable visitor/session data. Browser clients
-- use aggregate APIs; admin and alert queries run through trusted backends.
ALTER TABLE public.behavior_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.behavior_events FROM PUBLIC;

DO $$
DECLARE
    reader TEXT;
    columns_sql TEXT;
BEGIN
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO columns_sql
      FROM pg_attribute
     WHERE attrelid = 'public.behavior_events'::regclass
       AND attnum > 0 AND NOT attisdropped;

    -- Table-level REVOKE does not remove independent column-level grants.
    EXECUTE format('REVOKE SELECT (%1$s), INSERT (%1$s), UPDATE (%1$s), REFERENCES (%1$s) ON public.behavior_events FROM PUBLIC', columns_sql);
    FOREACH reader IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader) THEN
            EXECUTE format('REVOKE ALL ON public.behavior_events FROM %I', reader);
            EXECUTE format('REVOKE SELECT (%1$s), INSERT (%1$s), UPDATE (%1$s), REFERENCES (%1$s) ON public.behavior_events FROM %2$I', columns_sql, reader);
        END IF;
    END LOOP;
END $$;

-- No client policies: RLS defaults to deny. Preserve the table owner's and
-- existing service_role privileges for ingestion, rollups and protected APIs.
