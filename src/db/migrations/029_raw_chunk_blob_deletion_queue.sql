CREATE TABLE IF NOT EXISTS raw_chunk_blob_deletion_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL,
  blob_store TEXT NOT NULL,
  blob_key TEXT NOT NULL,
  queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at TIMESTAMPTZ,
  last_error TEXT,
  UNIQUE (blob_store, blob_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_chunk_blob_deletion_queue_pending
  ON raw_chunk_blob_deletion_queue (vault_id, queued_at)
  WHERE deleted_at IS NULL;
