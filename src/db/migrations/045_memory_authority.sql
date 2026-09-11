ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS authority_state TEXT NOT NULL DEFAULT 'proposed',
  ADD COLUMN IF NOT EXISTS approved_by TEXT,
  ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS approval_source TEXT,
  ADD COLUMN IF NOT EXISTS revoked_by TEXT,
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS authority_required BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS authority_version INTEGER NOT NULL DEFAULT 1 CHECK (authority_version > 0);

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_authority_state_check;
ALTER TABLE memories
  ADD CONSTRAINT memories_authority_state_check
  CHECK (authority_state IN ('untrusted', 'proposed', 'approved', 'revoked'));

UPDATE memories
SET authority_required = true
WHERE type IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
   OR status IS DISTINCT FROM 'active'
   OR archived_at IS NOT NULL;

CREATE OR REPLACE FUNCTION preserve_behavioral_memory_authority_requirement()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- Every current creation path is driven by a vault client or model output.
    -- Do not let its self-declared type decide whether approval is required.
    NEW.authority_required := true;
    NEW.authority_state := 'proposed';
    NEW.authority_version := 1;
    NEW.approved_by := NULL;
    NEW.approved_at := NULL;
    NEW.approval_source := NULL;
    NEW.revoked_by := NULL;
    NEW.revoked_at := NULL;
  ELSIF NEW.data IS DISTINCT FROM OLD.data
    OR NEW.subject IS DISTINCT FROM OLD.subject
    OR NEW.subject_encrypted IS DISTINCT FROM OLD.subject_encrypted
    OR NEW.subject_hmac IS DISTINCT FROM OLD.subject_hmac
    OR NEW.categories IS DISTINCT FROM OLD.categories
    OR NEW.type IS DISTINCT FROM OLD.type
    OR NEW.scope IS DISTINCT FROM OLD.scope
    OR NEW.evidence IS DISTINCT FROM OLD.evidence
    OR NEW.source_chunks IS DISTINCT FROM OLD.source_chunks
    OR NEW.source_segment_id IS DISTINCT FROM OLD.source_segment_id
  THEN
    -- Prompt-bearing mutations must not inherit an earlier approval, including
    -- rewrites of factual rows grandfathered by this migration.
    NEW.authority_required := true;
    NEW.authority_state := 'proposed';
    NEW.authority_version := OLD.authority_version + 1;
    NEW.approved_by := NULL;
    NEW.approved_at := NULL;
    NEW.approval_source := NULL;
    NEW.revoked_by := NULL;
    NEW.revoked_at := NULL;
  ELSE
    NEW.authority_required := COALESCE(OLD.authority_required, false)
      OR COALESCE(NEW.authority_required, false)
      OR COALESCE(NEW.type IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint'), false);
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS memories_preserve_authority_requirement ON memories;
CREATE TRIGGER memories_preserve_authority_requirement
BEFORE INSERT OR UPDATE OF data, subject, subject_encrypted, subject_hmac, categories, type, scope, evidence, source_chunks, source_segment_id, authority_required ON memories
FOR EACH ROW EXECUTE FUNCTION preserve_behavioral_memory_authority_requirement();

-- Only factual memories that were already active and unarchived retain their
-- historical recall behavior. Every previously non-recallable row enters the
-- authority lifecycle so a later status or archive transition cannot grant
-- authority to legacy model output. New rows fail closed regardless of type.
ALTER TABLE memories
  ALTER COLUMN authority_required SET DEFAULT true;

CREATE TABLE IF NOT EXISTS memory_authority_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  memory_id UUID NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('approve', 'revoke', 'invalidate', 'migration')),
  old_state TEXT CHECK (old_state IS NULL OR old_state IN ('untrusted', 'proposed', 'approved', 'revoked')),
  new_state TEXT NOT NULL CHECK (new_state IN ('untrusted', 'proposed', 'approved', 'revoked')),
  old_version BIGINT,
  new_version BIGINT NOT NULL,
  actor_type TEXT NOT NULL CHECK (actor_type IN ('api_key', 'service', 'system', 'user', 'worker')),
  actor_id TEXT,
  source TEXT NOT NULL CHECK (source IN ('api', 'curation_worker', 'extraction_worker', 'import', 'migration', 'system')),
  reason TEXT NOT NULL,
  snapshot JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (memory_id, event_type, new_version)
);

ALTER TABLE memory_authority_events
  DROP CONSTRAINT IF EXISTS memory_authority_events_actor_type_check;
ALTER TABLE memory_authority_events
  ADD CONSTRAINT memory_authority_events_actor_type_check
  CHECK (actor_type IN ('api_key', 'service', 'system', 'user', 'worker'));

-- Migration 043 predates delegated service identity. Upgrade its released audit
-- schema here so scope and authority events preserve the same actor classes.
ALTER TABLE memory_scope_change_log
  DROP CONSTRAINT IF EXISTS memory_scope_change_log_actor_type_check;
ALTER TABLE memory_scope_change_log
  ADD CONSTRAINT memory_scope_change_log_actor_type_check
  CHECK (actor_type IN ('api_key', 'service', 'system', 'user', 'worker'));

CREATE INDEX IF NOT EXISTS idx_memory_authority_events_memory_created
  ON memory_authority_events (vault_id, memory_id, created_at DESC);

INSERT INTO memory_authority_events (
  vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
  actor_type, source, reason, snapshot
)
-- Record the migration disposition of every legacy row placed in the authority
-- lifecycle. Legacy global-rule recall remains limited by the snapshot predicate
-- to content that could actually enter prompts when this migration ran.
SELECT vault_id, id, 'migration', NULL, 'proposed', NULL, authority_version,
       'system', 'migration',
       CASE
         WHEN type = 'user_rule'
          AND scope = 'global'
          AND status = 'active'
          AND archived_at IS NULL
           THEN 'Existing global rule requires explicit review; approval was not grandfathered.'
         ELSE 'Existing behavioral or non-recallable memory requires explicit review before future recall.'
       END,
       jsonb_build_object(
         'data', data,
         'subject', subject,
         'subject_encrypted', subject_encrypted,
         'subject_hmac', subject_hmac,
         'hash', hash,
         'categories', categories,
         'confidence', confidence,
         'score', score,
         'salience', salience,
         'sensitivity', sensitivity,
         'type', type,
         'authority_required', authority_required,
         'scope', scope,
         'polarity', polarity,
         'status', status,
         'evidence', evidence,
         'source_chunks', source_chunks,
         'source_segment_id', source_segment_id,
         'source_timestamp', source_timestamp,
         'valid_from', valid_from,
         'valid_until', valid_until,
         'parent_id', parent_id,
         'volatility', volatility,
         'last_recalled', last_recalled,
         'recall_count', recall_count,
         'created_at', created_at,
         'updated_at', updated_at,
         'archived_at', archived_at
       )
FROM memories
WHERE authority_required
ON CONFLICT (memory_id, event_type, new_version) DO NOTHING;

CREATE OR REPLACE FUNCTION reject_memory_authority_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' AND NOT EXISTS (
    SELECT 1 FROM vaults WHERE id = OLD.vault_id
  ) THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'memory_authority_events is append-only';
END;
$$;

DROP TRIGGER IF EXISTS memory_authority_events_append_only ON memory_authority_events;
CREATE TRIGGER memory_authority_events_append_only
BEFORE UPDATE OR DELETE ON memory_authority_events
FOR EACH ROW EXECUTE FUNCTION reject_memory_authority_event_mutation();
