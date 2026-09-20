-- Stop/drain cursor-aware schedulers first; run in a transaction. Preserve any
-- in-flight progress until drained, or explicitly approve resetting those
-- reminders to their start under a separate procedure. Never erase it silently.
LOCK TABLE memory_contradiction_schedule IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM memory_contradiction_schedule WHERE scan_memory_id IS NOT NULL) THEN
    RAISE EXCEPTION 'Cannot roll back bounded contradiction progress with retained cursors; drain or explicitly reset them first';
  END IF;
END $$;
DROP TRIGGER memory_contradiction_schedule_reset_cursor ON memory_contradiction_schedule;
DROP FUNCTION reset_contradiction_scan_cursor();
ALTER TABLE memory_contradiction_schedule
  DROP CONSTRAINT memory_contradiction_scan_cursor_shape,
  DROP COLUMN scan_reference_date,
  DROP COLUMN scan_similarity,
  DROP COLUMN scan_memory_id;
