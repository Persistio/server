CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  api_key_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  settings JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw_chunks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  embedding vector(1536),
  created_at TIMESTAMPTZ DEFAULT now(),
  processed BOOLEAN DEFAULT false
);

CREATE TABLE IF NOT EXISTS memories (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  data TEXT NOT NULL,
  subject TEXT NOT NULL,
  hash TEXT NOT NULL,
  embedding vector(1536),
  source_chunks UUID[] DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  last_recalled TIMESTAMPTZ,
  recall_count INT DEFAULT 0,
  confidence FLOAT DEFAULT 1.0,
  archived_at TIMESTAMPTZ,
  categories TEXT[] DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_tenant_processed
  ON raw_chunks (tenant_id, processed);

CREATE INDEX IF NOT EXISTS idx_memories_tenant_active
  ON memories (tenant_id, archived_at)
  WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_memories_subject
  ON memories (subject);

-- TODO: Add HNSW indexes for vector columns when memory count exceeds ~10k
-- CREATE INDEX ON raw_chunks USING hnsw (embedding vector_cosine_ops);
-- CREATE INDEX ON memories USING hnsw (embedding vector_cosine_ops);
