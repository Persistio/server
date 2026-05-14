DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dead_letter_has_source'
      AND conrelid = 'extraction_dead_letter'::regclass
  ) THEN
    ALTER TABLE extraction_dead_letter
      ADD CONSTRAINT dead_letter_has_source
      CHECK (chunk_id IS NOT NULL OR segment_id IS NOT NULL);
  END IF;
END $$;
