DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'memories'
      AND column_name = 'predicate'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'memories'
      AND column_name = 'type'
  ) THEN
    ALTER TABLE memories RENAME COLUMN predicate TO type;
  END IF;
END $$;

ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS type TEXT;

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_predicate_check;

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_status_check;

ALTER TABLE memories
  ADD CONSTRAINT memories_status_check
  CHECK (status IN ('active', 'superseded', 'contradicted', 'needs_review', 'candidate'));

-- Normalize curation_queue uniqueness to multi-tenant composite constraint
ALTER TABLE curation_queue DROP CONSTRAINT IF EXISTS curation_queue_segment_id_key;
ALTER TABLE curation_queue DROP CONSTRAINT IF EXISTS curation_queue_vault_segment_unique;
ALTER TABLE curation_queue ADD CONSTRAINT curation_queue_vault_segment_unique UNIQUE (vault_id, segment_id);

CREATE INDEX IF NOT EXISTS idx_curation_queue_vault_enqueued
  ON curation_queue (vault_id, enqueued_at)
  WHERE claimed_at IS NULL;
