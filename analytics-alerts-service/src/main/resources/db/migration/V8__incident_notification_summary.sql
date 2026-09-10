-- Preserve the visit evidence used by an alert across notification retries.
alter table incidents add column if not exists notification_summary text;
