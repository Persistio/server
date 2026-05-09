ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_predicate_check;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'memories'
      AND column_name = 'predicate'
  ) THEN
    ALTER TABLE memories RENAME COLUMN predicate TO type;
  END IF;
END $$;

ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS type TEXT;

ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'global',
  ADD COLUMN IF NOT EXISTS evidence JSONB;

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_type_check;

ALTER TABLE memories
  ADD CONSTRAINT memories_type_check
  CHECK (type IS NULL OR type IN (
    'user_preference', 'user_rule', 'task_pattern', 'workflow',
    'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge'
  ));

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_scope_check;

ALTER TABLE memories
  ADD CONSTRAINT memories_scope_check
  CHECK (scope IN ('global', 'project', 'task', 'session'));

CREATE TABLE IF NOT EXISTS memory_edges (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  from_memory_id UUID NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  to_memory_id UUID NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  type TEXT NOT NULL CHECK (type IN (
    'applies_to', 'part_of', 'depends_on', 'supports',
    'contradicts', 'supersedes', 'refines', 'relevant_when'
  )),
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.8,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (from_memory_id, to_memory_id, type)
);

CREATE INDEX IF NOT EXISTS idx_memory_edges_vault_from
  ON memory_edges (vault_id, from_memory_id);

CREATE INDEX IF NOT EXISTS idx_memory_edges_vault_to
  ON memory_edges (vault_id, to_memory_id);

CREATE INDEX IF NOT EXISTS idx_memories_vault_type_active
  ON memories (vault_id, type)
  WHERE status = 'active' AND archived_at IS NULL;

CREATE TABLE IF NOT EXISTS memory_embeddings (
  memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
  embedding vector(1536),
  embedding_model TEXT NOT NULL DEFAULT 'text-embedding-3-small',
  embedded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill embeddings for existing memories (no-op on fresh databases)
INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
SELECT id, embedding, COALESCE(updated_at, created_at)
FROM memories
WHERE embedding IS NOT NULL
ON CONFLICT (memory_id) DO NOTHING;
