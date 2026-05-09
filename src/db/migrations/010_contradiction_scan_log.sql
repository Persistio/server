CREATE TABLE IF NOT EXISTS contradiction_scan_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  memory_id_a UUID NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  memory_id_b UUID NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  decision TEXT NOT NULL CHECK (decision IN ('supersede_old', 'discard_new', 'needs_review', 'merge')),
  similarity DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contradiction_scan_log_vault_created
  ON contradiction_scan_log (vault_id, created_at DESC);
