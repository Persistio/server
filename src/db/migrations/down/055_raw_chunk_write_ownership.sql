-- Run only with writers and cleaners stopped/drained. Never erase an unresolved
-- fence to make a downgrade succeed. The migration runner must use a transaction.
LOCK TABLE raw_chunk_blob_write_intents, raw_chunks IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM raw_chunk_blob_write_intents) THEN
    RAISE EXCEPTION 'Cannot roll back raw upload ownership while write intents remain; preserve and reconcile them first';
  END IF;
END;
$$;
DROP TRIGGER raw_chunk_write_ownership ON raw_chunks;
DROP FUNCTION enforce_raw_chunk_write_ownership();
DROP INDEX idx_raw_chunk_write_intents_retry;
DROP INDEX idx_raw_chunk_write_intents_vault;
ALTER TABLE raw_chunk_blob_write_intents DROP COLUMN write_phase, DROP COLUMN revoked;
