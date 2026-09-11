-- A scope label is not an applicability boundary without a bound identity.
-- Historical non-global rows remain intact for incident analysis, but recall
-- predicates introduced with this migration treat a missing key as ineligible.
ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS scope_key TEXT;

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_scope_key_check;
ALTER TABLE memories
  ADD CONSTRAINT memories_scope_key_check CHECK (
    (scope = 'global' AND scope_key IS NULL)
    OR (
      scope <> 'global'
      AND (
        scope_key IS NULL
        OR (
          scope_key = btrim(scope_key)
          AND length(scope_key) BETWEEN 1 AND 512
          AND scope_key !~ '[[:cntrl:]]'
        )
      )
    )
  );

CREATE OR REPLACE FUNCTION enforce_new_memory_scope_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.scope IS NULL OR NEW.scope NOT IN ('global', 'project', 'task', 'session') THEN
    RETURN NEW;
  END IF;
  IF NEW.scope = 'global' THEN
    IF NEW.scope_key IS NOT NULL THEN
      RAISE EXCEPTION 'global memories must not have a scope_key';
    END IF;
  ELSIF NEW.scope_key IS NULL AND NEW.status <> 'needs_review' THEN
    RAISE EXCEPTION 'non-global memories require a scope_key';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS memories_enforce_new_scope_binding ON memories;
CREATE TRIGGER memories_enforce_new_scope_binding
BEFORE INSERT ON memories
FOR EACH ROW EXECUTE FUNCTION enforce_new_memory_scope_binding();

CREATE OR REPLACE FUNCTION enforce_changed_memory_scope_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.scope IS NULL OR NEW.scope NOT IN ('global', 'project', 'task', 'session') THEN
    RETURN NEW;
  END IF;
  -- Permit unrelated maintenance on preserved legacy rows, but never permit a
  -- scope or binding mutation to create another unbound active memory.
  IF NEW.scope IS DISTINCT FROM OLD.scope
    OR NEW.scope_key IS DISTINCT FROM OLD.scope_key
    OR (NEW.status = 'active' AND OLD.status IS DISTINCT FROM 'active')
  THEN
    IF NEW.scope = 'global' AND NEW.scope_key IS NOT NULL THEN
      RAISE EXCEPTION 'global memories must not have a scope_key';
    ELSIF NEW.scope <> 'global' AND NEW.scope_key IS NULL AND NEW.status <> 'needs_review' THEN
      RAISE EXCEPTION 'non-global memories require a scope_key';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS memories_enforce_changed_scope_binding ON memories;
CREATE TRIGGER memories_enforce_changed_scope_binding
BEFORE UPDATE OF scope, scope_key, status ON memories
FOR EACH ROW EXECUTE FUNCTION enforce_changed_memory_scope_binding();

CREATE INDEX IF NOT EXISTS idx_memories_vault_scope_binding
  ON memories (vault_id, scope, scope_key)
  WHERE archived_at IS NULL;

-- Entity aliases participate in extraction prompts and subject arbitration, so
-- they require the same applicability boundary as the memories they describe.
-- Existing vault-global rows are retained with a NULL scope for forensic value,
-- but fail closed because all runtime lookups require an exact non-NULL binding.
ALTER TABLE entity_aliases
  ADD COLUMN IF NOT EXISTS scope TEXT,
  ADD COLUMN IF NOT EXISTS scope_key TEXT;

ALTER TABLE entity_aliases
  DROP CONSTRAINT IF EXISTS entity_aliases_vault_id_alias_key;
ALTER TABLE entity_aliases
  DROP CONSTRAINT IF EXISTS entity_aliases_scope_binding_check;
ALTER TABLE entity_aliases
  ADD CONSTRAINT entity_aliases_scope_binding_check CHECK (CASE
    WHEN scope IS NULL THEN scope_key IS NULL
    WHEN scope = 'global' THEN scope_key IS NULL
    WHEN scope IN ('project', 'task', 'session') THEN
      scope_key IS NOT NULL
      AND scope_key = btrim(scope_key)
      AND length(scope_key) BETWEEN 1 AND 512
      AND scope_key !~ '[[:cntrl:]]'
    ELSE false
  END);

ALTER TABLE entity_aliases
  DROP CONSTRAINT IF EXISTS entity_aliases_vault_scope_binding_alias_key;
ALTER TABLE entity_aliases
  ADD CONSTRAINT entity_aliases_vault_scope_binding_alias_key
  UNIQUE NULLS NOT DISTINCT (vault_id, scope, scope_key, alias);

DROP INDEX IF EXISTS idx_entity_aliases_vault_canonical;
CREATE INDEX idx_entity_aliases_vault_scope_canonical
  ON entity_aliases (vault_id, scope, scope_key, canonical);

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS project_id TEXT,
  ADD COLUMN IF NOT EXISTS task_id TEXT,
  ADD COLUMN IF NOT EXISTS agent_id TEXT,
  ADD COLUMN IF NOT EXISTS trigger_type TEXT;

ALTER TABLE segments
  DROP CONSTRAINT IF EXISTS segments_context_identity_check;
ALTER TABLE segments
  ADD CONSTRAINT segments_context_identity_check CHECK (
    (project_id IS NULL OR (project_id = btrim(project_id) AND length(project_id) BETWEEN 1 AND 512 AND project_id !~ '[[:cntrl:]]'))
    AND (task_id IS NULL OR (task_id = btrim(task_id) AND length(task_id) BETWEEN 1 AND 512 AND task_id !~ '[[:cntrl:]]'))
    AND (agent_id IS NULL OR (agent_id = btrim(agent_id) AND length(agent_id) BETWEEN 1 AND 512 AND agent_id !~ '[[:cntrl:]]'))
    AND (trigger_type IS NULL OR trigger_type IN ('direct', 'delegated', 'scheduled', 'event', 'backfill', 'api', 'unknown'))
  );

ALTER TABLE memory_scope_change_log
  ADD COLUMN IF NOT EXISTS old_scope_key TEXT,
  ADD COLUMN IF NOT EXISTS new_scope_key TEXT;

-- Binding changes are authority changes too. Reinstall the trigger so direct
-- SQL and future writers cannot retain approval across an applicability change.
CREATE OR REPLACE FUNCTION preserve_behavioral_memory_authority_requirement()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
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
    OR NEW.scope_key IS DISTINCT FROM OLD.scope_key
    OR NEW.evidence IS DISTINCT FROM OLD.evidence
    OR NEW.source_chunks IS DISTINCT FROM OLD.source_chunks
    OR NEW.source_segment_id IS DISTINCT FROM OLD.source_segment_id
  THEN
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
BEFORE INSERT OR UPDATE OF data, subject, subject_encrypted, subject_hmac, categories, type, scope, scope_key, evidence, source_chunks, source_segment_id, authority_required ON memories
FOR EACH ROW EXECUTE FUNCTION preserve_behavioral_memory_authority_requirement();
