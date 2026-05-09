CREATE TABLE IF NOT EXISTS extraction_queue (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chunk_id    UUID NOT NULL REFERENCES raw_chunks(id),
  vault_id    UUID NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at  TIMESTAMPTZ,
  claimed_by  TEXT,
  retry_count INT NOT NULL DEFAULT 0,
  last_error  TEXT
);
CREATE INDEX IF NOT EXISTS idx_extraction_queue_vault_enqueued
  ON extraction_queue (vault_id, enqueued_at)
  WHERE claimed_at IS NULL;
