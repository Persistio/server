-- Only after disabling dispatch/capture and draining all request workers.
-- Business queue rows are retained. Re-enable through reconciliation after reapply.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM worker_wake_intents WHERE active_generation IS NOT NULL) THEN
    RAISE EXCEPTION 'Drain active wake cycles before removing the wake outbox';
  END IF;
END $$;
DROP TABLE worker_wake_intents;
