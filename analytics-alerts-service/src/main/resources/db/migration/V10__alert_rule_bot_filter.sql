-- Preserve existing rules; traffic filtering is an explicit, audited rule change.
ALTER TABLE alert_rules
    ADD COLUMN bot_filter varchar(16) NOT NULL DEFAULT 'ALL'
    CHECK (bot_filter IN ('ALL', 'EXCLUDE', 'ONLY'));
