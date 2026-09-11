CREATE TABLE IF NOT EXISTS curation_review_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  segment_id UUID NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  model TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  prompt_hash TEXT NOT NULL CHECK (prompt_hash ~ '^[0-9a-f]{64}$'),
  validation_status TEXT NOT NULL CHECK (validation_status IN ('valid', 'invalid', 'application_failed', 'applied')),
  validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(validation_errors) = 'array'),
  raw_response JSONB NOT NULL,
  before_state JSONB,
  after_state JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_curation_review_runs_vault_created
  ON curation_review_runs (vault_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_curation_review_runs_segment_created
  ON curation_review_runs (segment_id, created_at DESC);

-- Existing malformed legacy rows must not block deployment, but every new or
-- changed row must have a coherent validity interval. A later legacy audit can
-- repair old rows and validate this constraint online.
ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_validity_window_order;
ALTER TABLE memories
  ADD CONSTRAINT memories_validity_window_order
  CHECK (valid_from IS NULL OR valid_until IS NULL OR valid_from <= valid_until)
  NOT VALID;

CREATE OR REPLACE FUNCTION enforce_memory_activation_policy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'active' THEN
    IF (NEW.archived_at IS NOT NULL
        AND (TG_OP = 'INSERT' OR OLD.archived_at IS NOT NULL))
      OR (TG_OP = 'UPDATE' AND OLD.archived_at IS NOT NULL AND NEW.archived_at IS NULL)
    THEN
      RAISE EXCEPTION 'archived memory cannot be activated';
    END IF;
    IF NEW.sensitivity = 'restricted' THEN
      RAISE EXCEPTION 'restricted memory cannot be activated';
    END IF;
    IF NEW.confidence IS NULL OR NEW.confidence <= 0 OR NEW.confidence > 1 THEN
      RAISE EXCEPTION 'memory with invalid confidence cannot be activated';
    END IF;
    IF NEW.source_timestamp IS NOT NULL AND NEW.source_timestamp > now() + interval '5 minutes' THEN
      RAISE EXCEPTION 'memory with future source timestamp cannot be activated';
    END IF;
    IF jsonb_typeof(NEW.evidence) = 'object'
      AND NEW.evidence ? 'policy_rejections'
      AND (CASE
        WHEN jsonb_typeof(NEW.evidence -> 'policy_rejections') = 'array'
          THEN jsonb_array_length(NEW.evidence -> 'policy_rejections') > 0
        ELSE true
      END)
    THEN
      RAISE EXCEPTION 'policy-quarantined memory cannot be activated';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS memories_enforce_activation_policy ON memories;
CREATE TRIGGER memories_enforce_activation_policy
BEFORE INSERT OR UPDATE OF status, archived_at, sensitivity, confidence, source_timestamp, evidence ON memories
FOR EACH ROW EXECUTE FUNCTION enforce_memory_activation_policy();
