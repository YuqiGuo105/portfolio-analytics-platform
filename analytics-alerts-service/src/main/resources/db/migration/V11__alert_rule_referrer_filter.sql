-- Empty means all sources. Exact domains are evaluated against event facts,
-- since the existing geo rollup intentionally does not retain this dimension.
alter table alert_rules add column referrer_domain text not null default '';
