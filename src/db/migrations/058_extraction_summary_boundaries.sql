-- Stop/drain all extraction writers before applying. Old binaries select by only
-- vault/session and cannot safely coexist with boundary-aware summary readers.
-- Retain legacy summaries with NULL boundary: their source scope cannot be proven,
-- even when the currently surviving raw records are unbound. Never infer it.
LOCK TABLE session_contexts, segments IN ACCESS EXCLUSIVE MODE;
ALTER TABLE session_contexts ADD COLUMN context_boundary JSONB;
ALTER TABLE session_contexts ADD CONSTRAINT session_contexts_boundary_shape CHECK (
  context_boundary IS NULL OR CASE WHEN jsonb_typeof(context_boundary) = 'array' THEN
    jsonb_array_length(context_boundary) = 2
    AND jsonb_typeof(context_boundary->0) IN ('string', 'null')
    AND jsonb_typeof(context_boundary->1) IN ('string', 'null')
    AND (context_boundary->>0 IS NULL OR length(context_boundary->>0) BETWEEN 1 AND 512)
    AND (context_boundary->>1 IS NULL OR length(context_boundary->>1) BETWEEN 1 AND 512)
  ELSE false END
);
ALTER TABLE session_contexts DROP CONSTRAINT session_contexts_vault_id_session_id_key;
-- Digest only the bounded canonical JSONB representation to avoid PostgreSQL's
-- btree tuple limit for two maximum-length Unicode identities. Every read also
-- compares the full boundary; a digest collision can fail closed, never cross it.
CREATE UNIQUE INDEX session_contexts_boundary_unique ON session_contexts
  (vault_id, session_id, (digest(context_boundary::text, 'sha256')))
  WHERE context_boundary IS NOT NULL;
CREATE UNIQUE INDEX session_contexts_legacy_unique ON session_contexts(vault_id, session_id)
  WHERE context_boundary IS NULL;

-- Segment membership and capture bindings are accepted-source metadata, just as
-- raw_chunks.capture_context is. Never let a retry/restoration relabel evidence.
-- Keep the auxiliary encrypted context field independently updatable.
CREATE FUNCTION preserve_segment_source_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (NEW.vault_id, NEW.session_id, NEW.chunk_ids, NEW.project_id, NEW.task_id,
      NEW.agent_id, NEW.trigger_type, NEW.created_at)
    IS DISTINCT FROM (OLD.vault_id, OLD.session_id, OLD.chunk_ids, OLD.project_id, OLD.task_id,
      OLD.agent_id, OLD.trigger_type, OLD.created_at)
  THEN RAISE EXCEPTION 'Accepted segment source metadata is immutable'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER segments_preserve_source_metadata BEFORE UPDATE ON segments
  FOR EACH ROW EXECUTE FUNCTION preserve_segment_source_metadata();
