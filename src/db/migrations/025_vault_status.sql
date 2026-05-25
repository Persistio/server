ALTER TABLE vaults
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'vaults_status_check'
  ) THEN
    ALTER TABLE vaults
      ADD CONSTRAINT vaults_status_check
      CHECK (status IN ('active', 'disabled'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_vaults_active_api_key_hash
  ON vaults (api_key_hash)
  WHERE status = 'active';
