-- Existing writers did not record terminal PUT outcomes. Preserve uncertainty;
-- neither age nor a successful DELETE certifies an earlier PUT has finished.
ALTER TABLE raw_chunk_blob_write_intents
  ADD COLUMN write_phase TEXT NOT NULL DEFAULT 'uploading'
    CHECK (write_phase IN ('prepared', 'uploading', 'uploaded')),
  ADD COLUMN revoked BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX idx_raw_chunk_write_intents_retry
  ON raw_chunk_blob_write_intents (updated_at, id);
CREATE INDEX idx_raw_chunk_write_intents_vault
  ON raw_chunk_blob_write_intents (vault_id);

-- Older application instances do not inspect revoked. Enforce the cleanup fence
-- in SQL as well so rolling deployment cannot insert a reference after deletion.
CREATE FUNCTION enforce_raw_chunk_write_ownership() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE write_revoked BOOLEAN;
BEGIN
  SELECT revoked INTO write_revoked
  FROM raw_chunk_blob_write_intents
  WHERE blob_store = NEW.blob_store AND blob_key = NEW.blob_key
  FOR UPDATE;
  IF write_revoked IS TRUE THEN
    RAISE EXCEPTION 'Raw chunk upload ownership has been revoked' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER raw_chunk_write_ownership
  BEFORE INSERT OR UPDATE OF blob_store, blob_key ON raw_chunks
  FOR EACH ROW EXECUTE FUNCTION enforce_raw_chunk_write_ownership();
