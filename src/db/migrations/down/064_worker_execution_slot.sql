-- Disable request admission and prove all execution owners have stopped first.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM worker_execution_slot WHERE owner IS NOT NULL) THEN
    RAISE EXCEPTION 'Drain execution ownership before removing the slot';
  END IF;
END $$;
DROP TABLE worker_execution_slot;
