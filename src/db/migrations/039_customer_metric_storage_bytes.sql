ALTER TABLE raw_chunks
  ADD COLUMN IF NOT EXISTS storage_bytes BIGINT;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'raw_chunks'
      AND column_name = 'content'
  ) THEN
    UPDATE raw_chunks
    SET storage_bytes = octet_length(content::text)
    WHERE storage_bytes IS NULL
      AND content IS NOT NULL;
  END IF;
END $$;

ALTER TABLE raw_chunk_blob_deletion_queue
  ADD COLUMN IF NOT EXISTS workspace_id UUID,
  ADD COLUMN IF NOT EXISTS storage_bytes BIGINT;

CREATE INDEX IF NOT EXISTS idx_raw_chunk_blob_deletion_queue_workspace_pending
  ON raw_chunk_blob_deletion_queue (workspace_id, queued_at)
  WHERE deleted_at IS NULL
    AND workspace_id IS NOT NULL;
