-- Apply before starting cursor-aware schedulers. No memory/evidence rewrites.
-- Existing reminders start at the beginning; old schedulers ignore these fields
-- but must be stopped/drained to establish the bounded-pass operational guarantee.
LOCK TABLE memory_contradiction_schedule IN ACCESS EXCLUSIVE MODE;
ALTER TABLE memory_contradiction_schedule
  ADD COLUMN scan_reference_date DATE,
  ADD COLUMN scan_similarity DOUBLE PRECISION,
  ADD COLUMN scan_memory_id UUID,
  ADD CONSTRAINT memory_contradiction_scan_cursor_shape CHECK (
    (scan_reference_date IS NULL AND scan_similarity IS NULL AND scan_memory_id IS NULL)
    OR (scan_reference_date IS NOT NULL AND scan_similarity IS NOT NULL AND scan_memory_id IS NOT NULL
      AND scan_reference_date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'
      AND scan_similarity BETWEEN -1 AND 1)
  );

-- 057 replaces the generation on every substantive write. Bind progress to that
-- exact generation/revision without editing the deployed function. Counter-only
-- updates retain progress; a reset never touches customer memory or pair logs.
CREATE FUNCTION reset_contradiction_scan_cursor() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (NEW.generation, NEW.revision) IS DISTINCT FROM (OLD.generation, OLD.revision) THEN
    NEW.scan_reference_date := NULL;
    NEW.scan_similarity := NULL;
    NEW.scan_memory_id := NULL;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER memory_contradiction_schedule_reset_cursor
  BEFORE UPDATE ON memory_contradiction_schedule
  FOR EACH ROW EXECUTE FUNCTION reset_contradiction_scan_cursor();
