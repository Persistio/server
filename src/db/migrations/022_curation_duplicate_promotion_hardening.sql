ALTER TABLE curation_action_log
  DROP CONSTRAINT IF EXISTS curation_action_log_action_type_check;

ALTER TABLE curation_action_log
  ADD CONSTRAINT curation_action_log_action_type_check
  CHECK (action_type IN ('create', 'update', 'delete', 'promote', 'archive_duplicate'));

CREATE INDEX IF NOT EXISTS idx_memories_active_subject_lookup
  ON memories (vault_id, subject)
  WHERE archived_at IS NULL
    AND status = 'active'
    AND subject <> '';

CREATE INDEX IF NOT EXISTS idx_memories_active_subject_hmac_lookup
  ON memories (vault_id, subject_hmac)
  WHERE archived_at IS NULL
    AND status = 'active'
    AND subject_hmac IS NOT NULL;
