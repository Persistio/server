-- Drain old workers before upgrading: old binaries can send without reservation.
-- Extend the existing append-only contradiction log, not a new queue or ledger.
ALTER TABLE contradiction_scan_log
  ALTER COLUMN memory_id_b DROP NOT NULL,
  ADD COLUMN arbitration_job_key TEXT,
  ADD COLUMN send_attempt SMALLINT,
  ADD CONSTRAINT contradiction_attempt_shape CHECK (
    (arbitration_job_key IS NULL AND send_attempt IS NULL AND memory_id_b IS NOT NULL)
    OR (arbitration_job_key ~ '^[0-9a-f]{64}$' AND send_attempt BETWEEN 1 AND 3
      AND arbitration_job_key IS NOT NULL AND send_attempt IS NOT NULL AND decision='incomplete'));
ALTER TABLE contradiction_scan_log DROP CONSTRAINT contradiction_scan_log_decision_check;
ALTER TABLE contradiction_scan_log ADD CONSTRAINT contradiction_scan_log_decision_check
  CHECK(decision IN ('supersede_old','discard_new','keep_both','merge','incomplete')) NOT VALID;
CREATE UNIQUE INDEX contradiction_arbitration_send_slot
  ON contradiction_scan_log(vault_id,arbitration_job_key,send_attempt)
  WHERE arbitration_job_key IS NOT NULL;
CREATE OR REPLACE FUNCTION persistio_assert_contradiction_audit_references()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM memories WHERE id=NEW.memory_id_a AND vault_id=NEW.vault_id)
    OR (NEW.memory_id_b IS NOT NULL AND NOT EXISTS
      (SELECT 1 FROM memories WHERE id=NEW.memory_id_b AND vault_id=NEW.vault_id))
  THEN RAISE EXCEPTION 'contradiction audit memories must belong to vault %', NEW.vault_id USING ERRCODE='23503'; END IF;
  RETURN NEW;
END $$;
