-- Breaking contract replacement. This migration never resets customer data.
-- Quiesce old binaries and explicitly approve/reset the memory domain first.
LOCK TABLE memories, raw_chunks, segments, extraction_queue, curation_queue,
  memory_edges, entity_aliases, session_contexts IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM memories) OR EXISTS (SELECT 1 FROM raw_chunks)
    OR EXISTS (SELECT 1 FROM segments) OR EXISTS (SELECT 1 FROM extraction_queue)
    OR EXISTS (SELECT 1 FROM curation_queue) OR EXISTS (SELECT 1 FROM memory_edges)
    OR EXISTS (SELECT 1 FROM entity_aliases) OR EXISTS (SELECT 1 FROM session_contexts)
  THEN
    RAISE EXCEPTION 'Platform restoration requires an approved empty memory domain; no data was changed';
  END IF;
END $$;

-- Delete the recall tracking system, not ingest receipts or mutation evidence.
DROP FUNCTION persistio_delivery_integrity(uuid, uuid);
DROP FUNCTION persistio_delivery_snapshot(memory_delivery_events);
DROP TABLE memory_delivery_pending;
DROP TABLE memory_delivery_acknowledgements;
DROP TABLE memory_delivery_events;
DROP TABLE memory_delivery_runs;
DROP FUNCTION persistio_guard_delivery_pending();
DROP FUNCTION persistio_guard_delivery_ack();
DROP FUNCTION persistio_start_delivery();
DROP FUNCTION persistio_guard_delivery_event();
DROP FUNCTION persistio_refresh_delivery_pending();
DROP FUNCTION persistio_complete_delivery();

DROP TRIGGER memories_schedule_contradiction_activation ON memories;
DROP TRIGGER memory_authority_events_schedule_contradiction_activation ON memory_authority_events;
DROP FUNCTION schedule_memory_contradiction_activation();
DROP FUNCTION schedule_authority_event_contradiction_activation();
DROP FUNCTION refresh_memory_contradiction_schedule(uuid, uuid);
DROP FUNCTION contradiction_authority_eligible(memories, text);
DROP TABLE memory_contradiction_schedule;
DROP TABLE memory_contradiction_pending_vaults;
DROP FUNCTION maintain_contradiction_pending_vault();

DROP TRIGGER memories_preserve_authority_requirement ON memories;
DROP FUNCTION preserve_behavioral_memory_authority_requirement();
DROP TRIGGER memories_enforce_new_scope_binding ON memories;
DROP TRIGGER memories_enforce_changed_scope_binding ON memories;
DROP FUNCTION enforce_new_memory_scope_binding();
DROP FUNCTION enforce_changed_memory_scope_binding();
DROP TRIGGER memories_enforce_activation_policy ON memories;
DROP FUNCTION enforce_memory_activation_policy();

-- Recompiled below; the SQL function's composite argument is the old row shape.
DROP FUNCTION persistio_memory_observability_state(memories);
ALTER TABLE memories
  DROP COLUMN authority_state, DROP COLUMN authority_required, DROP COLUMN authority_version,
  DROP COLUMN approved_by, DROP COLUMN approved_at, DROP COLUMN approval_source,
  DROP COLUMN revoked_by, DROP COLUMN revoked_at, DROP COLUMN last_decayed_at,
  ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE memories DROP CONSTRAINT memories_status_check;
ALTER TABLE memories ADD CONSTRAINT memories_status_check
  CHECK (status IN ('active', 'superseded', 'contradicted'));
ALTER TABLE memories DROP CONSTRAINT memories_scope_key_check;
ALTER TABLE memories ADD CONSTRAINT memories_scope_key_check CHECK (
  (scope = 'global' AND scope_key IS NULL) OR
  (scope IN ('project', 'task', 'session') AND scope_key IS NOT NULL
    AND scope_key = btrim(scope_key) AND length(scope_key) BETWEEN 1 AND 512
    AND scope_key !~ '[[:cntrl:]]')
);
ALTER TABLE memories DROP CONSTRAINT memories_validity_window_order;
ALTER TABLE memories ADD CONSTRAINT memories_validity_window_order
  CHECK (valid_from IS NULL OR valid_until IS NULL OR valid_from <= valid_until);

CREATE FUNCTION maintain_memory_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.revision := 1;
  ELSIF (to_jsonb(NEW) - ARRAY['revision','last_recalled','recall_count','updated_at']::text[])
    IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['revision','last_recalled','recall_count','updated_at']::text[])
  THEN
    NEW.revision := OLD.revision + 1;
  ELSE
    NEW.revision := OLD.revision;
  END IF;
  IF NEW.status = 'active' THEN
    IF NEW.sensitivity = 'restricted' OR NEW.confidence IS NULL
      OR NOT (NEW.confidence > 0 AND NEW.confidence <= 1)
      OR NEW.source_timestamp > clock_timestamp() + interval '5 minutes'
    THEN RAISE EXCEPTION 'Invalid active memory'; END IF;
    IF NEW.evidence ? 'policy_rejections' AND
      (CASE WHEN jsonb_typeof(NEW.evidence->'policy_rejections') = 'array'
        THEN jsonb_array_length(NEW.evidence->'policy_rejections') <> 0 ELSE true END)
    THEN RAISE EXCEPTION 'Rejected evidence cannot create active memory'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER memories_maintain_revision BEFORE INSERT OR UPDATE ON memories
  FOR EACH ROW EXECUTE FUNCTION maintain_memory_revision();

CREATE FUNCTION persistio_memory_observability_state(memory_row memories)
RETURNS JSONB LANGUAGE sql STABLE AS $$
  SELECT jsonb_strip_nulls(jsonb_build_object(
    'data_hash', encode(digest(memory_row.data, 'sha256'), 'hex'),
    'subject_hash', encode(digest(memory_row.subject, 'sha256'), 'hex'),
    'hash', memory_row.hash, 'categories', memory_row.categories,
    'confidence', memory_row.confidence, 'score', memory_row.score,
    'salience', memory_row.salience, 'sensitivity', memory_row.sensitivity,
    'type', memory_row.type, 'scope', memory_row.scope, 'scope_key', memory_row.scope_key,
    'polarity', memory_row.polarity, 'status', memory_row.status, 'revision', memory_row.revision,
    'evidence_hash', CASE WHEN memory_row.evidence IS NULL THEN NULL
      ELSE encode(digest(memory_row.evidence::text, 'sha256'), 'hex') END,
    'source_chunks', memory_row.source_chunks, 'source_segment_id', memory_row.source_segment_id,
    'source_timestamp', memory_row.source_timestamp, 'valid_from', memory_row.valid_from,
    'valid_until', memory_row.valid_until, 'parent_id', memory_row.parent_id,
    'volatility', memory_row.volatility, 'created_at', memory_row.created_at,
    'updated_at', memory_row.updated_at, 'archived_at', memory_row.archived_at
  ));
$$;
CREATE OR REPLACE FUNCTION persistio_classify_memory_mutation(old_row memories, new_row memories)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF old_row IS NULL THEN RETURN 'create'; END IF;
  IF new_row IS NULL THEN RETURN 'delete'; END IF;
  IF old_row.archived_at IS NULL AND new_row.archived_at IS NOT NULL THEN RETURN 'archive'; END IF;
  IF old_row.archived_at IS NOT NULL AND new_row.archived_at IS NULL THEN RETURN 'unarchive'; END IF;
  IF old_row.scope IS DISTINCT FROM new_row.scope OR old_row.scope_key IS DISTINCT FROM new_row.scope_key THEN RETURN 'scope_change'; END IF;
  IF old_row.status IS DISTINCT FROM new_row.status THEN RETURN 'status_change'; END IF;
  IF old_row.data IS DISTINCT FROM new_row.data OR old_row.subject IS DISTINCT FROM new_row.subject THEN RETURN 'content_change'; END IF;
  IF old_row.source_chunks IS DISTINCT FROM new_row.source_chunks
    OR old_row.source_segment_id IS DISTINCT FROM new_row.source_segment_id
    OR old_row.evidence IS DISTINCT FROM new_row.evidence THEN RETURN 'provenance_change'; END IF;
  RETURN 'metadata_change';
END $$;

-- Improvement membership describes work, never availability. The version is
-- intentionally not an FK to a mutable revision: stale work is discarded by lease.
ALTER TABLE curation_queue ALTER COLUMN segment_id DROP NOT NULL;
DROP INDEX IF EXISTS idx_memories_vault_segment_candidate;
ALTER TABLE segments DROP COLUMN curation_ready_at, DROP COLUMN curation_enqueued_at;
DROP FUNCTION public.least_privileged_memory_scope(text,text);
ALTER TABLE curation_queue DROP CONSTRAINT IF EXISTS curation_queue_vault_id_segment_id_key;
ALTER TABLE curation_queue DROP CONSTRAINT curation_queue_vault_segment_unique;
ALTER TABLE curation_queue ADD COLUMN work_key TEXT NOT NULL;
ALTER TABLE curation_queue ADD CONSTRAINT curation_queue_work_key_length CHECK(length(work_key) BETWEEN 1 AND 512);
ALTER TABLE curation_queue ADD CONSTRAINT curation_queue_work_unique UNIQUE(vault_id, work_key);
ALTER TABLE curation_queue ADD CONSTRAINT curation_queue_id_vault_unique UNIQUE(id, vault_id);
CREATE TABLE curation_queue_items (
  queue_id UUID NOT NULL,
  vault_id UUID NOT NULL,
  memory_id UUID NOT NULL,
  revision BIGINT NOT NULL CHECK (revision > 0),
  PRIMARY KEY(queue_id, memory_id),
  FOREIGN KEY(queue_id, vault_id) REFERENCES curation_queue(id, vault_id) ON DELETE CASCADE,
  FOREIGN KEY(memory_id, vault_id) REFERENCES memories(id, vault_id) ON DELETE CASCADE
);
CREATE INDEX curation_queue_items_memory ON curation_queue_items(vault_id, memory_id);
ALTER TABLE curation_review_runs ALTER COLUMN segment_id DROP NOT NULL;
ALTER TABLE curation_action_log ALTER COLUMN segment_id DROP NOT NULL;
ALTER TABLE curation_dead_letter ALTER COLUMN segment_id DROP NOT NULL;
ALTER TABLE curation_dead_letter ADD COLUMN work_key TEXT;
ALTER TABLE curation_dead_letter ADD COLUMN targets JSONB;
ALTER TABLE curation_dead_letter ADD COLUMN source_queue_id UUID;
ALTER TABLE curation_review_runs ADD CONSTRAINT curation_review_runs_id_vault_unique UNIQUE(id,vault_id);
ALTER TABLE curation_action_log ADD COLUMN review_run_id UUID;
ALTER TABLE curation_action_log ADD CONSTRAINT curation_action_log_review_vault_fk
  FOREIGN KEY(review_run_id,vault_id) REFERENCES curation_review_runs(id,vault_id) ON DELETE RESTRICT;
CREATE INDEX curation_action_log_review_run ON curation_action_log(review_run_id);
ALTER TABLE extraction_dead_letter ADD COLUMN source_queue_id UUID;
ALTER TABLE extraction_dead_letter ADD COLUMN context_chunk_ids UUID[];

CREATE OR REPLACE FUNCTION persistio_assert_segment_audit_reference()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM vaults WHERE id = NEW.vault_id)
    OR (NEW.segment_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM segments WHERE id = NEW.segment_id AND vault_id = NEW.vault_id))
  THEN RAISE EXCEPTION 'Invalid curation audit reference' USING ERRCODE = '23503'; END IF;
  RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION persistio_assert_curation_action_audit_references()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM vaults WHERE id = NEW.vault_id)
    OR (NEW.segment_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM segments WHERE id = NEW.segment_id AND vault_id = NEW.vault_id))
    OR (NEW.memory_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM memories WHERE id = NEW.memory_id AND vault_id = NEW.vault_id))
    OR (NEW.new_memory_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM memories WHERE id = NEW.new_memory_id AND vault_id = NEW.vault_id))
  THEN RAISE EXCEPTION 'Invalid curation action reference' USING ERRCODE = '23503'; END IF;
  RETURN NEW;
END $$;

ALTER TABLE raw_chunks ADD COLUMN acceptance_ordinal BIGINT GENERATED ALWAYS AS IDENTITY;
ALTER TABLE raw_chunks ADD COLUMN capture_context JSONB NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(capture_context)='object');
CREATE INDEX raw_chunks_accepted_context ON raw_chunks(vault_id, session_id,
  (capture_context->>'project_id'),(capture_context->>'task_id'),acceptance_ordinal DESC);
-- Eligibility precedes the eight-source limit. Keep tool/oversized/keyless rows
-- out of that access path rather than walking an unbounded ineligible prefix.
CREATE INDEX raw_chunks_eligible_context ON raw_chunks(vault_id, session_id,
  (capture_context->>'project_id'),(capture_context->>'task_id'),acceptance_ordinal DESC)
  WHERE role IN ('user','assistant') AND blob_key IS NOT NULL AND storage_bytes BETWEEN 0 AND 32768;
CREATE FUNCTION preserve_raw_source_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (NEW.vault_id,NEW.session_id,NEW.role,NEW.created_at,NEW.provenance,NEW.capture_context,
      NEW.acceptance_ordinal,NEW.source_event_key,NEW.source_event_payload_sha256,
      NEW.source_event_namespace,NEW.source_event_id,NEW.source_event_ordinal,NEW.source_message_id,
      NEW.blob_key,NEW.blob_store,NEW.storage_bytes)
    IS DISTINCT FROM (OLD.vault_id,OLD.session_id,OLD.role,OLD.created_at,OLD.provenance,OLD.capture_context,
      OLD.acceptance_ordinal,OLD.source_event_key,OLD.source_event_payload_sha256,
      OLD.source_event_namespace,OLD.source_event_id,OLD.source_event_ordinal,OLD.source_message_id,
      OLD.blob_key,OLD.blob_store,OLD.storage_bytes)
  THEN RAISE EXCEPTION 'Accepted source metadata is immutable'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER raw_chunks_preserve_source_metadata BEFORE UPDATE ON raw_chunks
  FOR EACH ROW EXECUTE FUNCTION preserve_raw_source_metadata();
ALTER TABLE extraction_queue ADD COLUMN context_chunk_ids UUID[];

-- One change schedule, independent of approval policies and applicability dates.
ALTER TABLE contradiction_scan_log DROP CONSTRAINT contradiction_scan_log_decision_check;
ALTER TABLE contradiction_scan_log ADD CONSTRAINT contradiction_scan_log_decision_check
  CHECK(decision IN ('supersede_old','discard_new','keep_both','merge')) NOT VALID;
ALTER TABLE contradiction_scan_log ADD COLUMN revision_a BIGINT;
ALTER TABLE contradiction_scan_log ADD COLUMN revision_b BIGINT;
CREATE INDEX contradiction_scan_log_revision_a ON contradiction_scan_log(vault_id,memory_id_a,revision_a,memory_id_b,revision_b);
CREATE INDEX contradiction_scan_log_revision_b ON contradiction_scan_log(vault_id,memory_id_b,revision_b,memory_id_a,revision_a);
CREATE TABLE memory_contradiction_schedule (
  memory_id UUID PRIMARY KEY,
  vault_id UUID NOT NULL,
  revision BIGINT NOT NULL CHECK (revision > 0),
  generation UUID NOT NULL DEFAULT gen_random_uuid(),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  failures INTEGER NOT NULL DEFAULT 0 CHECK(failures BETWEEN 0 AND 10),
  FOREIGN KEY(memory_id, vault_id) REFERENCES memories(id, vault_id) ON DELETE CASCADE
);
CREATE INDEX memory_contradiction_schedule_due ON memory_contradiction_schedule(vault_id, available_at, memory_id);
CREATE TABLE memory_contradiction_pending_vaults (
  vault_id UUID PRIMARY KEY REFERENCES vaults(id) ON DELETE CASCADE,
  pending_count BIGINT NOT NULL CHECK(pending_count >= 0),
  revision BIGINT NOT NULL DEFAULT 0,
  next_visit_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX memory_contradiction_pending_vaults_due ON memory_contradiction_pending_vaults(next_visit_at, vault_id)
  WHERE pending_count > 0;
CREATE FUNCTION maintain_contradiction_pending_vault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    UPDATE memory_contradiction_pending_vaults SET pending_count = pending_count - 1, revision = revision + 1
      WHERE vault_id = OLD.vault_id;
  ELSIF TG_OP = 'UPDATE' THEN
    UPDATE memory_contradiction_pending_vaults SET revision = revision + 1,
      next_visit_at = LEAST(next_visit_at, NEW.available_at) WHERE vault_id = NEW.vault_id;
  ELSE
    INSERT INTO memory_contradiction_pending_vaults(vault_id,pending_count,revision,next_visit_at)
      VALUES(NEW.vault_id,1,1,NEW.available_at)
      ON CONFLICT(vault_id) DO UPDATE SET
        pending_count = memory_contradiction_pending_vaults.pending_count + 1,
        revision = memory_contradiction_pending_vaults.revision + 1,
        next_visit_at = CASE WHEN memory_contradiction_pending_vaults.pending_count = 0 THEN EXCLUDED.next_visit_at
          ELSE LEAST(memory_contradiction_pending_vaults.next_visit_at, EXCLUDED.next_visit_at) END;
  END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER memory_contradiction_schedule_pending_vault
  AFTER INSERT OR UPDATE OR DELETE ON memory_contradiction_schedule
  FOR EACH ROW EXECUTE FUNCTION maintain_contradiction_pending_vault();
CREATE FUNCTION schedule_memory_contradiction_activation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_memory memories;
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.revision = OLD.revision THEN RETURN NULL; END IF;
  SELECT * INTO current_memory FROM memories WHERE id = NEW.id AND vault_id = NEW.vault_id FOR NO KEY UPDATE;
  IF NOT FOUND THEN RETURN NULL; END IF;
  IF current_memory.status = 'active' AND current_memory.archived_at IS NULL THEN
    INSERT INTO memory_contradiction_schedule(memory_id,vault_id,revision)
      VALUES(current_memory.id,current_memory.vault_id,current_memory.revision)
      ON CONFLICT(memory_id) DO UPDATE SET revision = EXCLUDED.revision,
        generation = gen_random_uuid(), available_at = now(), failures = 0;
  ELSE
    DELETE FROM memory_contradiction_schedule WHERE memory_id = current_memory.id AND vault_id = current_memory.vault_id;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER memories_schedule_contradiction_activation
  AFTER INSERT OR UPDATE ON memories DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION schedule_memory_contradiction_activation();

-- Recovery is technical retry evidence; no memory-review or approval workflow.
CREATE TABLE memory_job_recoveries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL,
  failure_kind TEXT NOT NULL CHECK(failure_kind IN ('extraction','curation')),
  failure_id UUID NOT NULL,
  queue_id UUID NOT NULL,
  actor_id TEXT NOT NULL CHECK(length(actor_id) BETWEEN 1 AND 512),
  reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 2000),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(failure_kind, failure_id)
);
CREATE TRIGGER memory_job_recoveries_append_only BEFORE UPDATE OR DELETE ON memory_job_recoveries
  FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();
CREATE TRIGGER memory_job_recoveries_truncate_protected BEFORE TRUNCATE ON memory_job_recoveries
  FOR EACH STATEMENT EXECUTE FUNCTION reject_memory_observability_mutation();
CREATE FUNCTION assert_memory_job_recovery_reference() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM vaults WHERE id=NEW.vault_id) THEN
    RAISE EXCEPTION 'Recovery vault is unavailable' USING ERRCODE='23503';
  END IF;
  IF NEW.failure_kind='extraction' THEN
    IF NOT EXISTS(SELECT 1 FROM extraction_dead_letter WHERE id=NEW.failure_id AND vault_id=NEW.vault_id)
      OR NOT EXISTS(SELECT 1 FROM extraction_queue WHERE id=NEW.queue_id AND vault_id=NEW.vault_id)
    THEN RAISE EXCEPTION 'Invalid extraction recovery reference' USING ERRCODE='23503'; END IF;
  ELSE
    IF NOT EXISTS(SELECT 1 FROM curation_dead_letter WHERE id=NEW.failure_id AND vault_id=NEW.vault_id)
      OR NOT EXISTS(SELECT 1 FROM curation_queue WHERE id=NEW.queue_id AND vault_id=NEW.vault_id)
    THEN RAISE EXCEPTION 'Invalid curation recovery reference' USING ERRCODE='23503'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER memory_job_recoveries_validate BEFORE INSERT ON memory_job_recoveries
  FOR EACH ROW EXECUTE FUNCTION assert_memory_job_recovery_reference();
