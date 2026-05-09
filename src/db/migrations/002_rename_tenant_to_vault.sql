DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'tenants'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'vaults'
  ) THEN
    ALTER TABLE tenants RENAME TO vaults;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'raw_chunks'
      AND column_name = 'tenant_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'raw_chunks'
      AND column_name = 'vault_id'
  ) THEN
    ALTER TABLE raw_chunks RENAME COLUMN tenant_id TO vault_id;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'memories'
      AND column_name = 'tenant_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'memories'
      AND column_name = 'vault_id'
  ) THEN
    ALTER TABLE memories RENAME COLUMN tenant_id TO vault_id;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'idx_raw_chunks_tenant_processed'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'idx_raw_chunks_vault_processed'
  ) THEN
    ALTER INDEX idx_raw_chunks_tenant_processed RENAME TO idx_raw_chunks_vault_processed;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'idx_memories_tenant_active'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'idx_memories_vault_active'
  ) THEN
    ALTER INDEX idx_memories_tenant_active RENAME TO idx_memories_vault_active;
  END IF;
END $$;

ALTER TABLE vaults ADD COLUMN IF NOT EXISTS account_id UUID;
ALTER TABLE vaults ADD COLUMN IF NOT EXISTS encrypted_dek TEXT;
ALTER TABLE vaults ADD COLUMN IF NOT EXISTS vault_encryption_enabled BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS plans (
  id TEXT PRIMARY KEY,
  limits JSONB NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO plans (id, limits) VALUES
  ('free', '{"memories_max": 500, "ingest_events_per_month": 100, "memory_adds_per_month": 500, "searches_per_month": 1000}'),
  ('starter', '{"memories_max": 5000, "ingest_events_per_month": 1000, "memory_adds_per_month": 5000, "searches_per_month": 10000}'),
  ('pro', '{"memories_max": 50000, "ingest_events_per_month": 10000, "memory_adds_per_month": 50000, "searches_per_month": 100000}')
ON CONFLICT DO NOTHING;

ALTER TABLE vaults ADD COLUMN IF NOT EXISTS plan_id TEXT NOT NULL DEFAULT 'free' REFERENCES plans(id);

CREATE TABLE IF NOT EXISTS vault_usage (
  vault_id UUID PRIMARY KEY REFERENCES vaults(id) ON DELETE CASCADE,
  period TEXT NOT NULL,
  ingest_events INT NOT NULL DEFAULT 0,
  memory_adds INT NOT NULL DEFAULT 0,
  searches INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT now()
);
