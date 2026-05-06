DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'memories'
  ) THEN
    ALTER TABLE memories ADD COLUMN IF NOT EXISTS source_timestamp TIMESTAMPTZ;
  END IF;
END $$;
