-- Drain upgraded writers first. Never erase committed retry identity to downgrade.
LOCK TABLE contradiction_scan_log IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM contradiction_scan_log WHERE dedup_input_key IS NOT NULL) THEN
    RAISE EXCEPTION 'Cannot downgrade with retained committed arbitration outcomes';
  END IF;
END $$;
DROP INDEX contradiction_committed_dedup_input;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT contradiction_attempt_shape,
  DROP CONSTRAINT contradiction_dedup_input_shape, DROP COLUMN dedup_input_key;
ALTER TABLE contradiction_scan_log ADD CONSTRAINT contradiction_attempt_shape CHECK (
  (arbitration_job_key IS NULL AND send_attempt IS NULL AND memory_id_b IS NOT NULL)
  OR (arbitration_job_key ~ '^[0-9a-f]{64}$' AND send_attempt BETWEEN 1 AND 3
    AND arbitration_job_key IS NOT NULL AND send_attempt IS NOT NULL AND decision='incomplete'));
