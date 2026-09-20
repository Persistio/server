-- Drain old writers before upgrade. This identifies already committed incoming
-- outcomes, not provider receipts; rolled-back applies still use the 060 allowance.
ALTER TABLE contradiction_scan_log ADD COLUMN dedup_input_key TEXT;
ALTER TABLE contradiction_scan_log DROP CONSTRAINT contradiction_attempt_shape;
ALTER TABLE contradiction_scan_log ADD CONSTRAINT contradiction_attempt_shape CHECK (
  (arbitration_job_key IS NULL AND send_attempt IS NULL
    AND (memory_id_b IS NOT NULL OR (dedup_input_key IS NOT NULL AND decision IN ('merge','discard_new'))))
  OR (arbitration_job_key ~ '^[0-9a-f]{64}$' AND send_attempt BETWEEN 1 AND 3
    AND arbitration_job_key IS NOT NULL AND send_attempt IS NOT NULL AND decision='incomplete'
    AND dedup_input_key IS NULL));
ALTER TABLE contradiction_scan_log ADD CONSTRAINT contradiction_dedup_input_shape CHECK (
  dedup_input_key IS NULL OR (dedup_input_key ~ '^[0-9a-f]{64}$' AND send_attempt IS NULL AND arbitration_job_key IS NULL
    AND decision IN ('merge','discard_new','supersede_old','keep_both')));
CREATE UNIQUE INDEX contradiction_committed_dedup_input
  ON contradiction_scan_log(vault_id,dedup_input_key) WHERE dedup_input_key IS NOT NULL;
-- Existing append-only and same-vault reference guards deliberately remain.
