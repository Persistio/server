-- An old application cannot safely update newly admitted quarantine rows.
-- Explicit repair/export is an operator decision, never a destructive rollback.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM memories WHERE valid_from > valid_until) THEN
    RAISE EXCEPTION 'Cannot roll back temporal quarantine while inverted intervals remain; preserve and explicitly repair them first';
  END IF;
END;
$$;
ALTER TABLE memories DROP CONSTRAINT memories_validity_window_order;
ALTER TABLE memories ADD CONSTRAINT memories_validity_window_order
  CHECK (valid_from IS NULL OR valid_until IS NULL OR valid_from <= valid_until) NOT VALID;
