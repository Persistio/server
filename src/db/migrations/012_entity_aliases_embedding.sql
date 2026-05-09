ALTER TABLE entity_aliases ADD COLUMN IF NOT EXISTS embedding vector(1536);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_embedding
  ON entity_aliases USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 100);
