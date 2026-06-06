DO $$
DECLARE
  target_dimensions INTEGER := COALESCE(NULLIF(current_setting('persistio.storage_embedding_dimensions', true), '')::INTEGER, 1536);
  target_type TEXT;
BEGIN
  IF target_dimensions = 1536 THEN
    PERFORM set_config('persistio.skip_migration_record', 'true', true);
    RETURN;
  END IF;

  IF target_dimensions < 1 THEN
    RAISE EXCEPTION 'persistio.storage_embedding_dimensions must be positive, got %', target_dimensions;
  END IF;

  IF target_dimensions > 2000 THEN
    RAISE EXCEPTION 'persistio.storage_embedding_dimensions must be <= 2000 for indexed pgvector columns, got %', target_dimensions;
  END IF;

  target_type := format('vector(%s)', target_dimensions);

  IF NOT EXISTS (
    SELECT 1
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname IN ('raw_chunks', 'memories', 'memory_embeddings', 'entity_aliases')
      AND a.attname = 'embedding'
      AND NOT a.attisdropped
      AND format_type(a.atttypid, a.atttypmod) <> target_type
  ) THEN
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1 FROM raw_chunks WHERE embedding IS NOT NULL
    UNION ALL
    SELECT 1 FROM memories WHERE embedding IS NOT NULL
    UNION ALL
    SELECT 1 FROM memory_embeddings WHERE embedding IS NOT NULL
    UNION ALL
    SELECT 1 FROM entity_aliases WHERE embedding IS NOT NULL
    LIMIT 1
  ) THEN
    RAISE EXCEPTION 'Cannot change embedding dimensions while stored embeddings exist. Use a fresh database or re-embed before applying STORAGE_EMBEDDING_DIMENSIONS=%.', target_dimensions;
  END IF;

  DROP INDEX IF EXISTS idx_memory_embeddings_embedding_hnsw;
  DROP INDEX IF EXISTS idx_raw_chunks_embedding_hnsw;
  DROP INDEX IF EXISTS idx_entity_aliases_embedding;

  EXECUTE format('ALTER TABLE raw_chunks ALTER COLUMN embedding TYPE vector(%s) USING embedding::vector(%s)', target_dimensions, target_dimensions);
  EXECUTE format('ALTER TABLE memories ALTER COLUMN embedding TYPE vector(%s) USING embedding::vector(%s)', target_dimensions, target_dimensions);
  EXECUTE format('ALTER TABLE memory_embeddings ALTER COLUMN embedding TYPE vector(%s) USING embedding::vector(%s)', target_dimensions, target_dimensions);
  EXECUTE format('ALTER TABLE entity_aliases ALTER COLUMN embedding TYPE vector(%s) USING embedding::vector(%s)', target_dimensions, target_dimensions);

  CREATE INDEX IF NOT EXISTS idx_entity_aliases_embedding
    ON entity_aliases USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

  CREATE INDEX IF NOT EXISTS idx_memory_embeddings_embedding_hnsw
    ON memory_embeddings USING hnsw (embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

  CREATE INDEX IF NOT EXISTS idx_raw_chunks_embedding_hnsw
    ON raw_chunks USING hnsw (embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;
END $$;
