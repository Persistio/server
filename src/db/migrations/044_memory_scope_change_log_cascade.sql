-- Migration 043 was released with an append-only trigger that also rejected
-- ON DELETE CASCADE when a vault was removed. Replacing the function in a new
-- migration upgrades databases that have already recorded 043 as applied.
CREATE OR REPLACE FUNCTION reject_memory_scope_change_log_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' AND NOT EXISTS (
    SELECT 1 FROM vaults WHERE id = OLD.vault_id
  ) THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'memory_scope_change_log is append-only';
END;
$$;
