-- Scope is an authority boundary. Omission must fail instead of inheriting the
-- historical vault-global default, including for direct SQL and future writers.
ALTER TABLE memories
  ALTER COLUMN scope DROP DEFAULT,
  ALTER COLUMN scope SET NOT NULL;

-- Migration 016 installed and validated the allowlist. Do not drop and rebuild
-- it here: validating a replacement would scan the full table while the startup
-- migration holds an ACCESS EXCLUSIVE lock. Fail closed if schema drift removed
-- the released constraint so an operator can repair that anomaly explicitly.
DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'memories'::regclass
      AND conname = 'memories_scope_check'
      AND contype = 'c'
      AND convalidated
      AND pg_get_constraintdef(oid, true) = $definition$CHECK (scope = ANY (ARRAY['global'::text, 'project'::text, 'task'::text, 'session'::text]))$definition$
  ) THEN
    RAISE EXCEPTION 'validated memories_scope_check constraint is required';
  END IF;
END
$migration$;

-- Automatic writers must make their decision from the row they have locked,
-- not from an earlier application snapshot. Invalid inputs return NULL so the
-- NOT NULL constraint rejects the write instead of guessing at visibility.
CREATE OR REPLACE FUNCTION public.least_privileged_memory_scope(
  current_scope TEXT,
  incoming_scope TEXT
)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT CASE
    WHEN current_scope IS NULL
      OR incoming_scope IS NULL
      OR current_scope NOT IN ('global', 'project', 'task', 'session')
      OR incoming_scope NOT IN ('global', 'project', 'task', 'session') THEN NULL
    WHEN current_scope = 'session' OR incoming_scope = 'session' THEN 'session'
    WHEN current_scope = 'task' OR incoming_scope = 'task' THEN 'task'
    WHEN current_scope = 'project' OR incoming_scope = 'project' THEN 'project'
    ELSE 'global'
  END
$$;
