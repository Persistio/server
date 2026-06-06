ALTER TABLE vaults
  ADD COLUMN IF NOT EXISTS type TEXT,
  ADD COLUMN IF NOT EXISTS custom_extraction_prompt TEXT,
  ADD COLUMN IF NOT EXISTS custom_curation_prompt TEXT;

UPDATE vaults
  SET type = 'general'
  WHERE type IS NOT NULL
    AND type NOT IN ('general', 'custom');

ALTER TABLE vaults
  DROP CONSTRAINT IF EXISTS vaults_type_check;

ALTER TABLE vaults
  ADD CONSTRAINT vaults_type_check
  CHECK (type IS NULL OR type IN ('general', 'custom'));

DO $$
BEGIN
  ALTER TABLE vaults
    DROP CONSTRAINT IF EXISTS vaults_custom_prompt_size_check;

  ALTER TABLE vaults
    ADD CONSTRAINT vaults_custom_prompt_size_check
    CHECK (
      (custom_extraction_prompt IS NULL OR octet_length(custom_extraction_prompt) <= 87420)
      AND (custom_curation_prompt IS NULL OR octet_length(custom_curation_prompt) <= 32040)
    );
END $$;
