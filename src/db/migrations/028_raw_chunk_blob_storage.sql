ALTER TABLE raw_chunks
  ADD COLUMN IF NOT EXISTS blob_store TEXT,
  ADD COLUMN IF NOT EXISTS blob_key TEXT,
  ADD COLUMN IF NOT EXISTS blob_migrated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS content_sha256 TEXT;

ALTER TABLE raw_chunks
  ALTER COLUMN content DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_chunks_blob_missing
  ON raw_chunks (id)
  WHERE blob_key IS NULL
    AND content IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_chunks_blob_key
  ON raw_chunks (blob_store, blob_key)
  WHERE blob_key IS NOT NULL;
