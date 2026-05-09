CREATE TABLE IF NOT EXISTS curation_dead_letter (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  segment_id UUID NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  retry_count INT NOT NULL,
  last_error TEXT,
  dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_curation_dead_letter_vault_dead_lettered
  ON curation_dead_letter (vault_id, dead_lettered_at DESC);

CREATE INDEX IF NOT EXISTS idx_curation_dead_letter_segment
  ON curation_dead_letter (segment_id, dead_lettered_at DESC);
