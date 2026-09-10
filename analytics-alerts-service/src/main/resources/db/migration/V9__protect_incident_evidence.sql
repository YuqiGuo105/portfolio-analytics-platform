-- Incidents and visit evidence are served only by the authenticated admin API.
alter table public.incidents enable row level security;
revoke all on public.incidents from public, anon, authenticated;
