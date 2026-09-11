-- Immutable, content-minimised evidence for every durable memory transition.
-- These ledgers intentionally have no foreign keys to mutable domain rows: deleting
-- a segment, source chunk, memory, or vault must not erase incident evidence.
CREATE TABLE IF NOT EXISTS memory_mutation_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL,
  memory_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  changed_fields TEXT[] NOT NULL,
  source TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  reason TEXT,
  database_user TEXT NOT NULL DEFAULT current_user,
  database_application TEXT NOT NULL DEFAULT COALESCE(
    NULLIF(current_setting('application_name', true), ''), 'unknown'
  ),
  statement_hash TEXT NOT NULL DEFAULT encode(digest(current_query(), 'sha256'), 'hex')
    CHECK (statement_hash ~ '^[0-9a-f]{64}$'),
  before_state JSONB,
  after_state JSONB,
  transaction_id BIGINT NOT NULL DEFAULT txid_current(),
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_memory_mutation_events_memory_time
  ON memory_mutation_events (vault_id, memory_id, occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_memory_mutation_events_type_time
  ON memory_mutation_events (event_type, occurred_at DESC);

CREATE TABLE IF NOT EXISTS memory_delivery_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL,
  query_hash TEXT NOT NULL CHECK (query_hash ~ '^[0-9a-f]{64}$'),
  response_format TEXT NOT NULL CHECK (response_format IN ('json', 'bundle', 'bundle_v2')),
  mode TEXT NOT NULL CHECK (mode IN ('agent', 'factual')),
  client_name TEXT CHECK (client_name IS NULL OR length(btrim(client_name)) BETWEEN 1 AND 100),
  client_version TEXT CHECK (client_version IS NULL OR length(btrim(client_version)) BETWEEN 1 AND 100),
  session_id TEXT CHECK (session_id IS NULL OR length(btrim(session_id)) BETWEEN 1 AND 512),
  project_id TEXT CHECK (project_id IS NULL OR length(btrim(project_id)) BETWEEN 1 AND 512),
  task_id TEXT CHECK (task_id IS NULL OR length(btrim(task_id)) BETWEEN 1 AND 512),
  agent_id TEXT CHECK (agent_id IS NULL OR length(btrim(agent_id)) BETWEEN 1 AND 512),
  trigger_type TEXT CHECK (trigger_type IS NULL OR trigger_type IN ('direct', 'delegated', 'scheduled', 'event', 'backfill', 'api', 'unknown')),
  top_k INTEGER NOT NULL CHECK (top_k > 0),
  min_similarity DOUBLE PRECISION NOT NULL CHECK (min_similarity BETWEEN 0 AND 1),
  global_rule_policy TEXT NOT NULL CHECK (global_rule_policy IN ('off', 'approved_only', 'legacy')),
  include_global_rules_requested BOOLEAN NOT NULL,
  include_global_rules_effective BOOLEAN NOT NULL,
  selected_count INTEGER NOT NULL CHECK (selected_count >= 0),
  global_selected_count INTEGER NOT NULL CHECK (
    global_selected_count >= 0
    AND global_selected_count <= selected_count
    AND (global_selected_count = 0 OR include_global_rules_effective)
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (id, vault_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_delivery_runs_vault_time
  ON memory_delivery_runs (vault_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_delivery_runs_global_time
  ON memory_delivery_runs (created_at DESC) WHERE global_selected_count > 0;

CREATE TABLE IF NOT EXISTS memory_delivery_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  delivery_id UUID NOT NULL,
  vault_id UUID NOT NULL,
  memory_id UUID NOT NULL,
  authority_version BIGINT NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('selected', 'returned', 'rendered', 'dropped')),
  section TEXT NOT NULL CHECK (section IN (
    'approved_preferences_and_rules', 'historical_facts', 'candidates', 'graph_context'
  )),
  memory_type TEXT CHECK (memory_type IS NULL OR memory_type IN (
    'user_preference', 'user_rule', 'task_pattern', 'workflow', 'project',
    'constraint', 'decision', 'system_fact', 'domain_knowledge'
  )),
  retrieval_reason TEXT NOT NULL CHECK (retrieval_reason IN (
    'semantic', 'graph', 'global_behavioral'
  )),
  scope TEXT NOT NULL CHECK (scope IN ('global', 'project', 'session', 'task')),
  scope_binding TEXT,
  authority_state TEXT NOT NULL CHECK (authority_state IN ('untrusted', 'proposed', 'approved', 'revoked')),
  authority_required BOOLEAN NOT NULL,
  authority_approval_valid BOOLEAN NOT NULL,
  similarity DOUBLE PRECISION CHECK (similarity IS NULL OR similarity BETWEEN 0 AND 1),
  drop_reason TEXT CHECK (drop_reason IS NULL OR drop_reason IN ('token_budget', 'duplicate', 'invalid', 'client_policy', 'empty_block')),
  render_target TEXT CHECK (render_target IS NULL OR render_target IN ('prompt_context', 'tool_response')),
  token_budget INTEGER,
  rendered_tokens INTEGER,
  truncated BOOLEAN,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (delivery_id, vault_id) REFERENCES memory_delivery_runs(id, vault_id) ON DELETE RESTRICT,
  UNIQUE (delivery_id, memory_id, stage),
  CHECK (rendered_tokens IS NULL OR token_budget IS NULL OR rendered_tokens <= token_budget),
  CHECK (NOT authority_approval_valid OR authority_state = 'approved'),
  CHECK (
    retrieval_reason <> 'global_behavioral'
    OR (
      scope = 'global'
      AND memory_type = 'user_rule'
      AND section IN ('approved_preferences_and_rules', 'historical_facts')
    )
  ),
  CHECK (
    section <> 'approved_preferences_and_rules'
    OR (memory_type IN ('user_rule', 'user_preference') AND authority_approval_valid)
  ),
  CHECK (
    (retrieval_reason = 'graph') = (section = 'graph_context')
  ),
  CHECK (
    (scope = 'global' AND scope_binding IS NULL)
    OR (scope <> 'global' AND scope_binding IS NOT NULL AND length(btrim(scope_binding)) BETWEEN 1 AND 512)
  ),
  CHECK (
    (stage = 'dropped' AND drop_reason IS NOT NULL AND render_target IS NOT NULL AND token_budget IS NOT NULL AND rendered_tokens IS NOT NULL AND truncated IS NOT NULL)
    OR (stage = 'rendered' AND drop_reason IS NULL AND render_target IS NOT NULL AND token_budget IS NOT NULL AND rendered_tokens IS NOT NULL AND truncated IS NOT NULL)
    OR (stage IN ('selected', 'returned') AND drop_reason IS NULL AND render_target IS NULL AND token_budget IS NULL AND rendered_tokens IS NULL AND truncated IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_memory_delivery_events_memory_time
  ON memory_delivery_events (vault_id, memory_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_delivery_events_delivery_stage
  ON memory_delivery_events (delivery_id, stage);
CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_delivery_events_one_terminal_outcome
  ON memory_delivery_events (delivery_id, memory_id)
  WHERE stage IN ('rendered', 'dropped');
CREATE INDEX IF NOT EXISTS idx_memory_delivery_events_unapproved_rendered
  ON memory_delivery_events (occurred_at DESC)
  WHERE stage = 'rendered'
    AND memory_type IN ('user_rule', 'user_preference', 'task_pattern', 'workflow', 'constraint')
    AND authority_approval_valid = false;

CREATE OR REPLACE FUNCTION reject_memory_observability_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER memory_mutation_events_append_only
BEFORE UPDATE OR DELETE ON memory_mutation_events
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

CREATE TRIGGER memory_delivery_runs_append_only
BEFORE UPDATE OR DELETE ON memory_delivery_runs
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

CREATE TRIGGER memory_delivery_events_append_only
BEFORE UPDATE OR DELETE ON memory_delivery_events
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

CREATE OR REPLACE FUNCTION persistio_memory_observability_state(memory_row memories)
RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_strip_nulls(jsonb_build_object(
    'data_hash', encode(digest(memory_row.data, 'sha256'), 'hex'),
    'subject_hash', encode(digest(memory_row.subject, 'sha256'), 'hex'),
    'hash', memory_row.hash,
    'categories', memory_row.categories,
    'confidence', memory_row.confidence,
    'score', memory_row.score,
    'salience', memory_row.salience,
    'sensitivity', memory_row.sensitivity,
    'type', memory_row.type,
    'scope', memory_row.scope,
    'scope_key', memory_row.scope_key,
    'polarity', memory_row.polarity,
    'status', memory_row.status,
    'authority_required', memory_row.authority_required,
    'authority_state', memory_row.authority_state,
    'authority_version', memory_row.authority_version,
    'approved_by', memory_row.approved_by,
    'approved_at', memory_row.approved_at,
    'approval_source', memory_row.approval_source,
    'revoked_by', memory_row.revoked_by,
    'revoked_at', memory_row.revoked_at,
    'evidence_hash', CASE WHEN memory_row.evidence IS NULL THEN NULL
      ELSE encode(digest(memory_row.evidence::text, 'sha256'), 'hex') END,
    'policy_rejections', CASE WHEN jsonb_typeof(memory_row.evidence) = 'object'
      THEN memory_row.evidence -> 'policy_rejections' ELSE NULL END,
    'source_chunks', memory_row.source_chunks,
    'source_segment_id', memory_row.source_segment_id,
    'source_timestamp', memory_row.source_timestamp,
    'valid_from', memory_row.valid_from,
    'valid_until', memory_row.valid_until,
    'parent_id', memory_row.parent_id,
    'volatility', memory_row.volatility,
    'created_at', memory_row.created_at,
    'updated_at', memory_row.updated_at,
    'archived_at', memory_row.archived_at
  ));
$$;

CREATE OR REPLACE FUNCTION persistio_classify_memory_mutation(old_row memories, new_row memories)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF old_row IS NULL THEN RETURN 'create'; END IF;
  IF new_row IS NULL THEN RETURN 'delete'; END IF;
  IF old_row.archived_at IS NULL AND new_row.archived_at IS NOT NULL THEN RETURN 'archive'; END IF;
  IF old_row.archived_at IS NOT NULL AND new_row.archived_at IS NULL THEN RETURN 'unarchive'; END IF;
  IF new_row.authority_state = 'approved' AND old_row.authority_state IS DISTINCT FROM 'approved' THEN RETURN 'approval'; END IF;
  IF new_row.authority_state = 'revoked' AND old_row.authority_state IS DISTINCT FROM 'revoked' THEN RETURN 'revocation'; END IF;
  IF old_row.scope IS DISTINCT FROM new_row.scope OR old_row.scope_key IS DISTINCT FROM new_row.scope_key THEN RETURN 'scope_change'; END IF;
  IF old_row.status IS DISTINCT FROM new_row.status THEN RETURN 'status_change'; END IF;
  IF old_row.confidence IS DISTINCT FROM new_row.confidence THEN RETURN 'decay'; END IF;
  IF old_row.data IS DISTINCT FROM new_row.data OR old_row.subject IS DISTINCT FROM new_row.subject THEN RETURN 'content_change'; END IF;
  IF old_row.source_chunks IS DISTINCT FROM new_row.source_chunks
    OR old_row.source_segment_id IS DISTINCT FROM new_row.source_segment_id
    OR old_row.evidence IS DISTINCT FROM new_row.evidence THEN RETURN 'provenance_change'; END IF;
  RETURN 'metadata_change';
END;
$$;

CREATE OR REPLACE FUNCTION record_memory_mutation_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  previous memories;
  current memories;
  event_vault_id UUID;
  event_memory_id UUID;
  changed TEXT[];
BEGIN
  IF TG_OP = 'INSERT' THEN current := NEW; event_vault_id := NEW.vault_id; event_memory_id := NEW.id;
  ELSIF TG_OP = 'DELETE' THEN previous := OLD; event_vault_id := OLD.vault_id; event_memory_id := OLD.id;
  ELSE previous := OLD; current := NEW; event_vault_id := NEW.vault_id; event_memory_id := NEW.id;
  END IF;

  -- Recall counters are represented in the delivery ledger and must not create
  -- a second, misleading mutation history entry.
  IF TG_OP = 'UPDATE'
    AND (to_jsonb(NEW) - 'last_recalled' - 'recall_count' - 'updated_at')
      = (to_jsonb(OLD) - 'last_recalled' - 'recall_count' - 'updated_at') THEN
    RETURN NEW;
  END IF;

  IF TG_OP = 'INSERT' OR TG_OP = 'DELETE' THEN
    changed := ARRAY['*'];
  ELSE
    SELECT array_agg(COALESCE(before_field.key, after_field.key) ORDER BY COALESCE(before_field.key, after_field.key))
    INTO changed
    FROM jsonb_each(to_jsonb(OLD) - 'embedding' - 'last_recalled' - 'recall_count') AS before_field
    FULL JOIN jsonb_each(to_jsonb(NEW) - 'embedding' - 'last_recalled' - 'recall_count') AS after_field
      ON after_field.key = before_field.key
    WHERE before_field.value IS DISTINCT FROM after_field.value;
  END IF;

  INSERT INTO memory_mutation_events (
    vault_id, memory_id, event_type, changed_fields, source, actor_type, actor_id, reason, before_state, after_state
  ) VALUES (
    event_vault_id,
    event_memory_id,
    persistio_classify_memory_mutation(previous, current),
    COALESCE(changed, '{}'::text[]),
    COALESCE(NULLIF(current_setting('persistio.mutation_source', true), ''), 'database'),
    COALESCE(NULLIF(current_setting('persistio.actor_type', true), ''), 'system'),
    NULLIF(current_setting('persistio.actor_id', true), ''),
    NULLIF(current_setting('persistio.mutation_reason', true), ''),
    CASE WHEN previous IS NULL THEN NULL ELSE persistio_memory_observability_state(previous) END,
    CASE WHEN current IS NULL THEN NULL ELSE persistio_memory_observability_state(current) END
  );
  RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER memories_record_mutation_event
AFTER INSERT OR UPDATE OR DELETE ON memories
FOR EACH ROW EXECUTE FUNCTION record_memory_mutation_event();

-- Existing curation evidence must survive deletion of the source segment and
-- source memory. Keep stable identifiers, but remove destructive cascades.
ALTER TABLE memory_authority_events DROP CONSTRAINT IF EXISTS memory_authority_events_vault_id_fkey;
ALTER TABLE memory_scope_change_log DROP CONSTRAINT IF EXISTS memory_scope_change_log_vault_id_fkey;

ALTER TABLE contradiction_scan_log DROP CONSTRAINT IF EXISTS contradiction_scan_log_vault_id_fkey;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT IF EXISTS contradiction_scan_log_memory_id_a_fkey;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT IF EXISTS contradiction_scan_log_memory_id_b_fkey;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT IF EXISTS contradiction_scan_memory_a_vault_fkey;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT IF EXISTS contradiction_scan_memory_b_vault_fkey;

ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_log_vault_id_fkey;
ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_log_segment_id_fkey;
ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_segment_vault_fkey;
ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_log_memory_id_fkey;
ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_log_new_memory_id_fkey;
ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_memory_vault_fkey;
ALTER TABLE curation_action_log DROP CONSTRAINT IF EXISTS curation_action_new_memory_vault_fkey;

ALTER TABLE curation_review_runs DROP CONSTRAINT IF EXISTS curation_review_runs_vault_id_fkey;
ALTER TABLE curation_review_runs DROP CONSTRAINT IF EXISTS curation_review_runs_segment_id_fkey;
ALTER TABLE curation_review_runs DROP CONSTRAINT IF EXISTS curation_review_segment_vault_fkey;

ALTER TABLE curation_dead_letter DROP CONSTRAINT IF EXISTS curation_dead_letter_vault_id_fkey;
ALTER TABLE curation_dead_letter DROP CONSTRAINT IF EXISTS curation_dead_letter_segment_id_fkey;
ALTER TABLE curation_dead_letter DROP CONSTRAINT IF EXISTS curation_dead_letter_segment_vault_fkey;

ALTER TABLE extraction_dead_letter DROP CONSTRAINT IF EXISTS extraction_dead_letter_vault_id_fkey;
ALTER TABLE extraction_dead_letter DROP CONSTRAINT IF EXISTS extraction_dead_letter_chunk_id_fkey;
ALTER TABLE extraction_dead_letter DROP CONSTRAINT IF EXISTS extraction_dead_letter_segment_id_fkey;
ALTER TABLE extraction_dead_letter DROP CONSTRAINT IF EXISTS extraction_dead_letter_job_id_fkey;

-- Replacing destructive foreign keys must not weaken point-in-time integrity.
-- These insert guards validate the same ownership relationships while the
-- domain rows exist, then let immutable evidence outlive them.
CREATE OR REPLACE FUNCTION persistio_assert_contradiction_audit_references()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM memories WHERE id = NEW.memory_id_a AND vault_id = NEW.vault_id)
    OR NOT EXISTS (SELECT 1 FROM memories WHERE id = NEW.memory_id_b AND vault_id = NEW.vault_id)
  THEN
    RAISE EXCEPTION 'contradiction audit memories must belong to vault %', NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER contradiction_scan_log_reference_guard
BEFORE INSERT ON contradiction_scan_log
FOR EACH ROW EXECUTE FUNCTION persistio_assert_contradiction_audit_references();

CREATE OR REPLACE FUNCTION persistio_assert_curation_action_audit_references()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM segments WHERE id = NEW.segment_id AND vault_id = NEW.vault_id)
    OR (NEW.memory_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM memories WHERE id = NEW.memory_id AND vault_id = NEW.vault_id
    ))
    OR (NEW.new_memory_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM memories WHERE id = NEW.new_memory_id AND vault_id = NEW.vault_id
    ))
  THEN
    RAISE EXCEPTION 'curation action references must belong to vault %', NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER curation_action_log_reference_guard
BEFORE INSERT ON curation_action_log
FOR EACH ROW EXECUTE FUNCTION persistio_assert_curation_action_audit_references();

CREATE OR REPLACE FUNCTION persistio_assert_segment_audit_reference()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM segments WHERE id = NEW.segment_id AND vault_id = NEW.vault_id) THEN
    RAISE EXCEPTION '% segment must belong to vault %', TG_TABLE_NAME, NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER curation_review_runs_reference_guard
BEFORE INSERT ON curation_review_runs
FOR EACH ROW EXECUTE FUNCTION persistio_assert_segment_audit_reference();

CREATE TRIGGER curation_dead_letter_reference_guard
BEFORE INSERT ON curation_dead_letter
FOR EACH ROW EXECUTE FUNCTION persistio_assert_segment_audit_reference();

CREATE OR REPLACE FUNCTION persistio_assert_extraction_dead_letter_references()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF (NEW.chunk_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM raw_chunks WHERE id = NEW.chunk_id AND vault_id = NEW.vault_id
    ))
    OR (NEW.segment_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM segments WHERE id = NEW.segment_id AND vault_id = NEW.vault_id
    ))
    OR (NEW.job_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM jobs WHERE id = NEW.job_id AND vault_id = NEW.vault_id
    ))
  THEN
    RAISE EXCEPTION 'extraction dead-letter references must belong to vault %', NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER extraction_dead_letter_reference_guard
BEFORE INSERT ON extraction_dead_letter
FOR EACH ROW EXECUTE FUNCTION persistio_assert_extraction_dead_letter_references();

-- The older authority/scope triggers allowed deletes only to accommodate their
-- former vault cascades. Once audit evidence is independent, every mutation is
-- rejected. Decision and dead-letter logs are also immutable from this point.
DROP TRIGGER IF EXISTS memory_authority_events_append_only ON memory_authority_events;
CREATE TRIGGER memory_authority_events_append_only
BEFORE UPDATE OR DELETE ON memory_authority_events
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

DROP TRIGGER IF EXISTS memory_scope_change_log_append_only ON memory_scope_change_log;
CREATE TRIGGER memory_scope_change_log_append_only
BEFORE UPDATE OR DELETE ON memory_scope_change_log
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

CREATE TRIGGER contradiction_scan_log_append_only
BEFORE UPDATE OR DELETE ON contradiction_scan_log
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

CREATE TRIGGER curation_action_log_append_only
BEFORE UPDATE OR DELETE ON curation_action_log
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

-- Review rows have a deliberate valid -> applied/application_failed lifecycle,
-- so retain those fenced updates while preventing evidence removal.
CREATE OR REPLACE FUNCTION enforce_curation_review_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE'
    OR OLD.validation_status <> 'valid'
    OR OLD.id IS DISTINCT FROM NEW.id
    OR OLD.vault_id IS DISTINCT FROM NEW.vault_id
    OR OLD.segment_id IS DISTINCT FROM NEW.segment_id
    OR OLD.model IS DISTINCT FROM NEW.model
    OR OLD.schema_version IS DISTINCT FROM NEW.schema_version
    OR OLD.prompt_version IS DISTINCT FROM NEW.prompt_version
    OR OLD.prompt_hash IS DISTINCT FROM NEW.prompt_hash
    OR OLD.raw_response IS DISTINCT FROM NEW.raw_response
    OR OLD.created_at IS DISTINCT FROM NEW.created_at
  THEN
    RAISE EXCEPTION 'curation_review_runs audit mutation is not permitted';
  END IF;

  IF NEW.validation_status = 'valid' THEN
    IF OLD.validation_errors IS DISTINCT FROM NEW.validation_errors
      OR OLD.after_state IS DISTINCT FROM NEW.after_state
      OR OLD.applied_at IS DISTINCT FROM NEW.applied_at
    THEN
      RAISE EXCEPTION 'curation_review_runs valid phase may only refresh before_state';
    END IF;
  ELSIF NEW.validation_status = 'application_failed' THEN
    IF OLD.before_state IS DISTINCT FROM NEW.before_state
      OR OLD.after_state IS DISTINCT FROM NEW.after_state
      OR NEW.applied_at IS NOT NULL
    THEN
      RAISE EXCEPTION 'curation_review_runs failure transition is invalid';
    END IF;
  ELSIF NEW.validation_status = 'applied' THEN
    IF OLD.before_state IS DISTINCT FROM NEW.before_state
      OR OLD.validation_errors IS DISTINCT FROM NEW.validation_errors
      OR NEW.after_state IS NULL
      OR NEW.applied_at IS NULL
    THEN
      RAISE EXCEPTION 'curation_review_runs applied transition is invalid';
    END IF;
  ELSE
    RAISE EXCEPTION 'curation_review_runs terminal transition is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER curation_review_runs_delete_protected
BEFORE UPDATE OR DELETE ON curation_review_runs
FOR EACH ROW EXECUTE FUNCTION enforce_curation_review_audit_mutation();

CREATE TRIGGER curation_dead_letter_append_only
BEFORE UPDATE OR DELETE ON curation_dead_letter
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

CREATE TRIGGER extraction_dead_letter_append_only
BEFORE UPDATE OR DELETE ON extraction_dead_letter
FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

-- Capture the deployment baseline without pretending historical transitions are
-- reconstructable. Subsequent transitions are exact trigger-generated evidence.
INSERT INTO memory_mutation_events (
  vault_id, memory_id, event_type, changed_fields, source, actor_type, reason, after_state, transaction_id, occurred_at
)
SELECT vault_id, id, 'observation_baseline', ARRAY['*'], 'migration', 'system',
       'State observed when immutable mutation observability was enabled.',
       persistio_memory_observability_state(memories), txid_current(), clock_timestamp()
FROM memories;
