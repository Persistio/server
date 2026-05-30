ALTER TABLE extraction_queue
  ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal';

CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  error TEXT
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'extraction_queue_priority_check'
  ) THEN
    ALTER TABLE extraction_queue
      ADD CONSTRAINT extraction_queue_priority_check
      CHECK (priority IN ('normal', 'bulk'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'jobs_kind_check'
  ) THEN
    ALTER TABLE jobs
      ADD CONSTRAINT jobs_kind_check
      CHECK (kind IN ('bulk_ingest'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'jobs_status_check'
  ) THEN
    ALTER TABLE jobs
      ADD CONSTRAINT jobs_status_check
      CHECK (status IN ('queued', 'running', 'completed', 'failed'));
  END IF;
END $$;

ALTER TABLE extraction_queue
  ADD COLUMN IF NOT EXISTS job_id UUID REFERENCES jobs(id) ON DELETE SET NULL;

ALTER TABLE extraction_dead_letter
  ADD COLUMN IF NOT EXISTS job_id UUID REFERENCES jobs(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_extraction_queue_priority_enqueued
  ON extraction_queue (priority DESC, enqueued_at)
  WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_extraction_queue_job
  ON extraction_queue (job_id)
  WHERE job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_vault_created
  ON jobs (vault_id, created_at DESC);
