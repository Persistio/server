ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS curation_ready_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS curation_enqueued_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_segments_curation_ready
  ON segments (vault_id, curation_ready_at)
  WHERE curation_ready_at IS NOT NULL;
