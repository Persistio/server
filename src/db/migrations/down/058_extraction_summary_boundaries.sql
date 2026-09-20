-- Stop/drain writers and run in a transaction. A downgrade must never silently
-- relabel a bound summary as session-only or erase a retained legacy summary.
-- Export/preserve and explicitly remove summaries under a separately approved
-- procedure first. Old binaries are unsafe for reused project/task sessions.
LOCK TABLE session_contexts, segments IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM session_contexts) THEN
    RAISE EXCEPTION 'Cannot roll back summary isolation with retained summaries; preserve and explicitly clear them first';
  END IF;
END $$;
DROP TRIGGER segments_preserve_source_metadata ON segments;
DROP FUNCTION preserve_segment_source_metadata();
DROP INDEX session_contexts_boundary_unique;
DROP INDEX session_contexts_legacy_unique;
ALTER TABLE session_contexts DROP COLUMN context_boundary;
ALTER TABLE session_contexts ADD CONSTRAINT session_contexts_vault_id_session_id_key UNIQUE(vault_id, session_id);
