ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_status_check;

ALTER TABLE memories
  ADD CONSTRAINT memories_status_check
  CHECK (status IN ('active', 'candidate', 'superseded', 'contradicted', 'needs_review'));

ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS source_segment_id UUID REFERENCES segments(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_memories_source_segment
  ON memories(source_segment_id)
  WHERE source_segment_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS curation_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  segment_id UUID NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at TIMESTAMPTZ,
  claimed_by TEXT,
  retry_count INT NOT NULL DEFAULT 0,
  last_error TEXT,
  UNIQUE (vault_id, segment_id)
);

CREATE INDEX IF NOT EXISTS idx_curation_queue_vault_enqueued
  ON curation_queue (vault_id, enqueued_at)
  WHERE claimed_at IS NULL;

CREATE TABLE IF NOT EXISTS curation_action_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  segment_id UUID NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL CHECK (action_type IN ('create', 'update', 'delete', 'promote')),
  memory_id UUID REFERENCES memories(id),
  new_memory_id UUID REFERENCES memories(id),
  subject TEXT,
  old_value TEXT,
  new_value TEXT,
  raw_curator_response JSONB NOT NULL,
  triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at TIMESTAMPTZ,
  error TEXT
);

CREATE INDEX IF NOT EXISTS idx_curation_action_log_vault_triggered
  ON curation_action_log (vault_id, triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_curation_action_log_segment
  ON curation_action_log (segment_id);

CREATE INDEX IF NOT EXISTS idx_curation_action_log_memory
  ON curation_action_log (memory_id)
  WHERE memory_id IS NOT NULL;
