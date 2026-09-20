-- Stop/drain upgraded workers first. Never erase attempt history to downgrade.
LOCK TABLE contradiction_scan_log IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM contradiction_scan_log WHERE decision='incomplete' OR send_attempt IS NOT NULL) THEN
    RAISE EXCEPTION 'Cannot downgrade with retained arbitration assessments or send reservations';
  END IF;
END $$;
DROP INDEX contradiction_arbitration_send_slot;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT contradiction_attempt_shape,
  DROP CONSTRAINT contradiction_scan_log_decision_check,
  DROP COLUMN arbitration_job_key, DROP COLUMN send_attempt,
  ALTER COLUMN memory_id_b SET NOT NULL;
ALTER TABLE contradiction_scan_log ADD CONSTRAINT contradiction_scan_log_decision_check
  CHECK(decision IN ('supersede_old','discard_new','keep_both','merge')) NOT VALID;
CREATE OR REPLACE FUNCTION persistio_assert_contradiction_audit_references()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM memories WHERE id=NEW.memory_id_a AND vault_id=NEW.vault_id)
    OR NOT EXISTS (SELECT 1 FROM memories WHERE id=NEW.memory_id_b AND vault_id=NEW.vault_id)
  THEN RAISE EXCEPTION 'contradiction audit memories must belong to vault %', NEW.vault_id USING ERRCODE='23503'; END IF;
  RETURN NEW;
END $$;
