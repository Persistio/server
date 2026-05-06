ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS salience NUMERIC(3,2) NOT NULL DEFAULT 0.5,
  ADD COLUMN IF NOT EXISTS sensitivity TEXT NOT NULL DEFAULT 'low',
  ADD COLUMN IF NOT EXISTS predicate TEXT,
  ADD COLUMN IF NOT EXISTS polarity TEXT NOT NULL DEFAULT 'neutral',
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS valid_from DATE,
  ADD COLUMN IF NOT EXISTS valid_until DATE;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'memories_sensitivity_check'
  ) THEN
    ALTER TABLE memories
      ADD CONSTRAINT memories_sensitivity_check
      CHECK (sensitivity IN ('low', 'medium', 'high', 'restricted'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'memories_predicate_check'
  ) THEN
    ALTER TABLE memories
      ADD CONSTRAINT memories_predicate_check
      CHECK (predicate IS NULL OR predicate IN ('preference', 'fact', 'plan', 'relationship', 'constraint', 'event'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'memories_polarity_check'
  ) THEN
    ALTER TABLE memories
      ADD CONSTRAINT memories_polarity_check
      CHECK (polarity IN ('positive', 'negative', 'neutral'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'memories_status_check'
  ) THEN
    ALTER TABLE memories
      ADD CONSTRAINT memories_status_check
      CHECK (status IN ('active', 'superseded', 'contradicted', 'needs_review'));
  END IF;
END $$;
