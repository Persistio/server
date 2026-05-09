CREATE TABLE IF NOT EXISTS entity_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  alias TEXT NOT NULL CHECK (char_length(alias) <= 500),
  canonical TEXT NOT NULL CHECK (char_length(canonical) <= 500),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (vault_id, alias)
);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_vault_canonical
  ON entity_aliases (vault_id, canonical);
