ALTER TABLE vaults
  ADD COLUMN IF NOT EXISTS rate_limit_override JSONB;

CREATE TABLE IF NOT EXISTS extraction_dead_letter (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  chunk_id UUID REFERENCES raw_chunks(id) ON DELETE SET NULL,
  segment_id UUID REFERENCES segments(id) ON DELETE SET NULL,
  retry_count INT NOT NULL DEFAULT 0,
  last_error TEXT,
  dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_extraction_dead_letter_vault_dead_lettered
  ON extraction_dead_letter (vault_id, dead_lettered_at DESC);

CREATE INDEX IF NOT EXISTS idx_extraction_dead_letter_segment
  ON extraction_dead_letter (segment_id, dead_lettered_at DESC);
