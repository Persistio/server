CREATE INDEX IF NOT EXISTS idx_memories_vault_subject_active
  ON memories (vault_id, subject)
  WHERE archived_at IS NULL AND status = 'active';

CREATE INDEX IF NOT EXISTS idx_memories_vault_segment_candidate
  ON memories (vault_id, source_segment_id)
  WHERE archived_at IS NULL AND status = 'candidate' AND source_segment_id IS NOT NULL;
