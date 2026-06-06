CREATE TABLE IF NOT EXISTS vault_model_usage (
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  period TEXT NOT NULL,
  provider TEXT NOT NULL,
  model_role TEXT NOT NULL CHECK (model_role IN ('embedding', 'extraction', 'curation', 'escalation')),
  model TEXT NOT NULL,
  request_count BIGINT NOT NULL DEFAULT 0,
  embedding_calls BIGINT NOT NULL DEFAULT 0,
  embedding_input_tokens BIGINT NOT NULL DEFAULT 0,
  embedding_input_chars BIGINT NOT NULL DEFAULT 0,
  prompt_tokens BIGINT NOT NULL DEFAULT 0,
  completion_tokens BIGINT NOT NULL DEFAULT 0,
  total_tokens BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (vault_id, period, provider, model_role, model)
);

CREATE INDEX IF NOT EXISTS idx_vault_model_usage_period_role
  ON vault_model_usage (period, model_role, provider, model);
