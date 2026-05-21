CREATE INDEX IF NOT EXISTS idx_vaults_api_key_hash
  ON vaults (api_key_hash);

CREATE INDEX IF NOT EXISTS idx_memory_embeddings_embedding_hnsw
  ON memory_embeddings USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_chunks_embedding_hnsw
  ON raw_chunks USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;
