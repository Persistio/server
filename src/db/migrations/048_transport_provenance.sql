-- Preserve transport identity independently from content authorship and make
-- replayed source events idempotent at the tenant boundary.
ALTER TABLE raw_chunks
  ADD COLUMN IF NOT EXISTS source_event_namespace TEXT,
  ADD COLUMN IF NOT EXISTS source_event_id TEXT,
  ADD COLUMN IF NOT EXISTS source_message_id TEXT,
  ADD COLUMN IF NOT EXISTS source_event_key TEXT,
  ADD COLUMN IF NOT EXISTS ingest_job_id UUID REFERENCES jobs(id) ON DELETE SET NULL;

ALTER TABLE raw_chunks
  DROP CONSTRAINT IF EXISTS raw_chunks_source_event_identity_check;
ALTER TABLE raw_chunks
  ADD CONSTRAINT raw_chunks_source_event_identity_check CHECK (
    (
      source_event_namespace IS NULL
      AND source_event_id IS NULL
      AND source_message_id IS NULL
      AND source_event_key IS NULL
    )
    OR (
      source_event_namespace IS NOT NULL
      AND source_event_id IS NOT NULL
      AND source_event_key IS NOT NULL
      AND source_event_namespace = btrim(source_event_namespace)
      AND source_event_id = btrim(source_event_id)
      AND length(source_event_namespace) BETWEEN 1 AND 256
      AND length(source_event_id) BETWEEN 1 AND 512
      AND (source_message_id IS NULL OR (
        source_message_id = btrim(source_message_id)
        AND length(source_message_id) BETWEEN 1 AND 512
        AND source_message_id !~ '[[:cntrl:]]'
      ))
      AND source_event_namespace !~ '[[:cntrl:]]'
      AND source_event_id !~ '[[:cntrl:]]'
      AND source_event_key ~ '^[0-9a-f]{64}$'
    )
  );

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_chunks_vault_source_event
  ON raw_chunks (vault_id, source_event_key)
  WHERE source_event_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_chunks_ingest_job
  ON raw_chunks (ingest_job_id)
  WHERE ingest_job_id IS NOT NULL;
