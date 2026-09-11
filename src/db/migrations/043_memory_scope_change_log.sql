CREATE TABLE IF NOT EXISTS memory_scope_change_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  memory_id UUID NOT NULL,
  old_scope TEXT NOT NULL CHECK (old_scope IN ('global', 'project', 'task', 'session')),
  new_scope TEXT NOT NULL CHECK (new_scope IN ('global', 'project', 'task', 'session')),
  actor_type TEXT NOT NULL CHECK (actor_type IN ('api_key', 'system', 'user', 'worker')),
  actor_id TEXT,
  source TEXT NOT NULL CHECK (source IN ('api', 'curation_worker', 'extraction_worker', 'import', 'system')),
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_memory_scope_change_log_memory_created
  ON memory_scope_change_log (vault_id, memory_id, created_at DESC);

CREATE OR REPLACE FUNCTION reject_memory_scope_change_log_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' AND NOT EXISTS (
    SELECT 1 FROM vaults WHERE id = OLD.vault_id
  ) THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'memory_scope_change_log is append-only';
END;
$$;

DROP TRIGGER IF EXISTS memory_scope_change_log_append_only ON memory_scope_change_log;
CREATE TRIGGER memory_scope_change_log_append_only
BEFORE UPDATE OR DELETE ON memory_scope_change_log
FOR EACH ROW EXECUTE FUNCTION reject_memory_scope_change_log_mutation();
