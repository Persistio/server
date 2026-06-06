DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM raw_chunks
    WHERE blob_key IS NULL
       OR blob_store IS NULL
  ) THEN
    RAISE EXCEPTION 'Cannot finalize raw chunk blob migration while unmigrated rows remain';
  END IF;
END $$;

ALTER TABLE raw_chunks
  ALTER COLUMN blob_store SET NOT NULL,
  ALTER COLUMN blob_key SET NOT NULL;

ALTER TABLE raw_chunks
  DROP COLUMN IF EXISTS content,
  DROP COLUMN IF EXISTS blob_migrated_at,
  DROP COLUMN IF EXISTS content_sha256;

DROP INDEX IF EXISTS idx_raw_chunks_blob_missing;
