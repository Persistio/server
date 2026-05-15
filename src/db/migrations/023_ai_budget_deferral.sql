UPDATE plans
SET limits = (limits - 'gemini_rpm' - 'gemini_tpm') || CASE id
  WHEN 'free' THEN '{"ai_requests_per_minute": 10, "ai_tokens_per_minute": 50000, "ai_extraction_weight": 1, "ai_escalation_weight": 2, "ai_curation_weight": 4}'::jsonb
  WHEN 'starter' THEN '{"ai_requests_per_minute": 50, "ai_tokens_per_minute": 250000, "ai_extraction_weight": 1, "ai_escalation_weight": 2, "ai_curation_weight": 4}'::jsonb
  WHEN 'pro' THEN '{"ai_requests_per_minute": 100, "ai_tokens_per_minute": 500000, "ai_extraction_weight": 1, "ai_escalation_weight": 2, "ai_curation_weight": 4}'::jsonb
  ELSE '{}'::jsonb
END
WHERE id IN ('free', 'starter', 'pro');

ALTER TABLE extraction_queue
  ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE curation_queue
  ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT now();

DROP INDEX IF EXISTS idx_extraction_queue_vault_enqueued;
CREATE INDEX IF NOT EXISTS idx_extraction_queue_available
  ON extraction_queue (available_at, enqueued_at)
  WHERE claimed_at IS NULL;

DROP INDEX IF EXISTS idx_curation_queue_vault_enqueued;
CREATE INDEX IF NOT EXISTS idx_curation_queue_available
  ON curation_queue (available_at, enqueued_at)
  WHERE claimed_at IS NULL;
