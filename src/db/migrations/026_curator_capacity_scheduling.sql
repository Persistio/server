ALTER TABLE vault_usage
  ADD COLUMN IF NOT EXISTS curator_runs INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS curator_requests INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS curator_input_tokens INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS curator_output_tokens INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS curator_candidates_processed INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS curator_candidates_deferred INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS vault_curation_state (
  vault_id UUID PRIMARY KEY REFERENCES vaults(id) ON DELETE CASCADE,
  last_curator_run_at TIMESTAMPTZ,
  next_curator_run_at TIMESTAMPTZ,
  last_curator_defer_reason TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE vault_curation_state
  ADD COLUMN IF NOT EXISTS curator_claimed_until TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS curator_claimed_by TEXT;

CREATE INDEX IF NOT EXISTS idx_vault_curation_state_next_run
  ON vault_curation_state (next_curator_run_at);

CREATE INDEX IF NOT EXISTS idx_vault_curation_state_claimed_until
  ON vault_curation_state (curator_claimed_until)
  WHERE curator_claimed_until IS NOT NULL;

UPDATE plans
SET limits = limits || '{
  "curator_enabled": true,
  "curator_schedule_interval_minutes": 60,
  "curator_runs_per_month": 1000,
  "curator_jobs_per_run": 20,
  "curator_candidates_per_run": 400,
  "curator_candidates_per_call": 20,
  "curator_active_memories_per_call": 80,
  "curator_input_tokens_per_call": 12000,
  "curator_output_tokens_per_call": 2000,
  "curator_tokens_per_month": 10000000,
  "curator_requests_per_month": 3000,
  "curator_max_queue_age_hours": 24,
  "curator_backlog_limit": 10000,
  "curator_priority_weight": 3.0,
  "curator_overage_mode": "payg"
}'::jsonb
WHERE id = 'unlimited';
