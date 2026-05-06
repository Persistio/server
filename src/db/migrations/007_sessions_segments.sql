CREATE TABLE IF NOT EXISTS session_contexts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL,
  context TEXT NOT NULL,
  extracted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (vault_id, session_id)
);

CREATE TABLE IF NOT EXISTS segments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL,
  chunk_ids UUID[] NOT NULL DEFAULT '{}',
  context TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE extraction_queue
  ADD COLUMN IF NOT EXISTS segment_id UUID REFERENCES segments(id) ON DELETE CASCADE;

ALTER TABLE extraction_queue
  ALTER COLUMN chunk_id DROP NOT NULL;

ALTER TABLE extraction_queue
  ADD CONSTRAINT extraction_queue_chunk_or_segment_not_null
  CHECK (chunk_id IS NOT NULL OR segment_id IS NOT NULL);

CREATE INDEX IF NOT EXISTS idx_session_contexts_vault_session
  ON session_contexts (vault_id, session_id);

CREATE INDEX IF NOT EXISTS idx_segments_vault_session
  ON segments (vault_id, session_id, created_at);

CREATE INDEX IF NOT EXISTS idx_extraction_queue_segment
  ON extraction_queue (segment_id)
  WHERE segment_id IS NOT NULL;
