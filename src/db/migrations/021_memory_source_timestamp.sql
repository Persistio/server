-- 0006 only adds this column when memories already exists. Fresh databases run
-- migrations in filename order, so this follow-up guarantees the column exists.
ALTER TABLE memories
  ADD COLUMN IF NOT EXISTS source_timestamp TIMESTAMPTZ;
