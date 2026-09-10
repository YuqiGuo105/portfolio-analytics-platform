# Behavior event access boundary

`public.behavior_events` contains canonical, but still linkable, visitor facts.
It is not a browser-facing dataset. Migration V15 enables RLS and revokes table
and column privileges from `PUBLIC`, `anon`, and `authenticated`. No client RLS
policies are installed. Signing in to Supabase alone does not grant admin access.

The existing trusted JDBC owner and server-only `service_role` retain access.
Ingestion, rollups, alert evidence, and authorized admin queries continue through
backend services. Public visitors use the bounded aggregate endpoints under
`/api/public/visits`; never expose backend credentials to restore direct reads.

## Verification

CI creates an isolated `analytics_test` PostgreSQL database and runs
`BehaviorEventAccessPostgresTest`. It upgrades the real Flyway schema from V14,
reproduces old grants, checks denied SQL operations under real client roles,
tests RLS defense in depth, and exercises ingestion plus public/admin HTTP paths.
The test refuses non-loopback URLs and any other database name.

After deployment, verify RLS and both table/column grants, request the Supabase
Data API with the public key and `select=event_id&limit=0` (must be denied), and
check the public summary and protected admin query endpoints. Never insert real
visitor events or send alert emails merely to test this permission change.

Rollback application code if necessary, but retain this database restriction.
Restore a failing trusted backend identity rather than regranting client access.
