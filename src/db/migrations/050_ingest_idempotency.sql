-- Make capture retries address the same source-event parts and leave durable
-- evidence for reconciling object writes that do not reach a SQL commit.
ALTER TABLE raw_chunks
  ADD COLUMN IF NOT EXISTS source_event_ordinal BIGINT,
  ADD COLUMN IF NOT EXISTS source_event_payload_sha256 TEXT;

-- Released source identities represented one whole event. Preserve their
-- existing keys as ordinal zero and use an explicit legacy hash sentinel until
-- a matching replay can attest the payload checksum.
UPDATE raw_chunks
SET source_event_ordinal = 0,
    source_event_payload_sha256 = repeat('0', 64)
WHERE source_event_key IS NOT NULL
  AND source_event_ordinal IS NULL;

ALTER TABLE raw_chunks
  DROP CONSTRAINT IF EXISTS raw_chunks_source_event_identity_check;
ALTER TABLE raw_chunks
  ADD CONSTRAINT raw_chunks_source_event_identity_check CHECK (
    (
      source_event_namespace IS NULL
      AND source_event_id IS NULL
      AND source_message_id IS NULL
      AND source_event_key IS NULL
      AND source_event_ordinal IS NULL
      AND source_event_payload_sha256 IS NULL
    )
    OR (
      source_event_namespace IS NOT NULL
      AND source_event_id IS NOT NULL
      AND source_event_key IS NOT NULL
      AND source_event_ordinal IS NOT NULL
      AND source_event_payload_sha256 IS NOT NULL
      AND source_event_namespace = btrim(source_event_namespace)
      AND source_event_id = btrim(source_event_id)
      AND length(source_event_namespace) BETWEEN 1 AND 256
      AND length(source_event_id) BETWEEN 1 AND 512
      AND source_event_ordinal BETWEEN 0 AND 9007199254740991
      AND (source_message_id IS NULL OR (
        source_message_id = btrim(source_message_id)
        AND length(source_message_id) BETWEEN 1 AND 512
        AND source_message_id !~ '[[:cntrl:]]'
      ))
      AND source_event_namespace !~ '[[:cntrl:]]'
      AND source_event_id !~ '[[:cntrl:]]'
      AND source_event_key ~ '^[0-9a-f]{64}$'
      AND source_event_payload_sha256 ~ '^[0-9a-f]{64}$'
    )
  );

CREATE TABLE IF NOT EXISTS raw_chunk_blob_write_intents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Deliberately not an FK: the cleanup record must survive vault deletion in
  -- the same way as raw_chunk_blob_deletion_queue.
  vault_id UUID NOT NULL,
  blob_store TEXT NOT NULL CHECK (blob_store IN ('local', 'azure_blob', 'gcs')),
  blob_key TEXT NOT NULL,
  source_event_key TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT,
  UNIQUE (blob_store, blob_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_chunk_blob_write_intents_stale
  ON raw_chunk_blob_write_intents (created_at, id);

ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_vault_kind_idempotency
  ON jobs (vault_id, kind, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
