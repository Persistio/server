-- Fence queue workers with renewable, per-claim capabilities. A worker name is
-- diagnostic only; claim_token is the authority required for every transition.
ALTER TABLE extraction_queue
  ADD COLUMN IF NOT EXISTS claim_token UUID,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

ALTER TABLE curation_queue
  ADD COLUMN IF NOT EXISTS claim_token UUID,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

ALTER TABLE vault_curation_state
  ADD COLUMN IF NOT EXISTS curator_claim_token UUID;

UPDATE extraction_queue
SET claimed_at = NULL, claimed_by = NULL, claim_token = NULL, lease_expires_at = NULL
WHERE claim_token IS NULL OR lease_expires_at IS NULL;

UPDATE curation_queue
SET claimed_at = NULL, claimed_by = NULL, claim_token = NULL, lease_expires_at = NULL
WHERE claim_token IS NULL OR lease_expires_at IS NULL;

UPDATE vault_curation_state
SET curator_claimed_until = NULL, curator_claimed_by = NULL, curator_claim_token = NULL
WHERE curator_claim_token IS NULL OR curator_claimed_until IS NULL;

ALTER TABLE extraction_queue
  DROP CONSTRAINT IF EXISTS extraction_queue_claim_state_check;
ALTER TABLE extraction_queue
  ADD CONSTRAINT extraction_queue_claim_state_check CHECK (
    (claimed_at IS NULL AND claimed_by IS NULL AND claim_token IS NULL AND lease_expires_at IS NULL)
    OR
    (claimed_at IS NOT NULL AND claimed_by IS NOT NULL AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
  );

ALTER TABLE curation_queue
  DROP CONSTRAINT IF EXISTS curation_queue_claim_state_check;
ALTER TABLE curation_queue
  ADD CONSTRAINT curation_queue_claim_state_check CHECK (
    (claimed_at IS NULL AND claimed_by IS NULL AND claim_token IS NULL AND lease_expires_at IS NULL)
    OR
    (claimed_at IS NOT NULL AND claimed_by IS NOT NULL AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
  );

ALTER TABLE vault_curation_state
  DROP CONSTRAINT IF EXISTS vault_curation_state_claim_check;
ALTER TABLE vault_curation_state
  ADD CONSTRAINT vault_curation_state_claim_check CHECK (
    (curator_claimed_until IS NULL AND curator_claimed_by IS NULL AND curator_claim_token IS NULL)
    OR
    (curator_claimed_until IS NOT NULL AND curator_claimed_by IS NOT NULL AND curator_claim_token IS NOT NULL)
  );

CREATE INDEX IF NOT EXISTS idx_extraction_queue_lease
  ON extraction_queue (lease_expires_at) WHERE claim_token IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_curation_queue_lease
  ON curation_queue (lease_expires_at) WHERE claim_token IS NOT NULL;

-- A receipt is committed in the same transaction as the corresponding durable
-- mutation. It survives queue deletion and makes crash/replay behaviour explicit.
CREATE TABLE IF NOT EXISTS worker_action_receipts (
  queue_kind TEXT NOT NULL CHECK (queue_kind IN ('extraction', 'curation')),
  queue_id UUID NOT NULL,
  action_key TEXT NOT NULL,
  claim_token UUID NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  accounted_at TIMESTAMPTZ,
  PRIMARY KEY (queue_kind, queue_id, action_key)
);

-- Add composite candidate keys so relationships can enforce that both ends
-- belong to the declared vault, not merely that each UUID exists somewhere.
ALTER TABLE raw_chunks ADD CONSTRAINT raw_chunks_id_vault_unique UNIQUE (id, vault_id);
ALTER TABLE segments ADD CONSTRAINT segments_id_vault_unique UNIQUE (id, vault_id);
ALTER TABLE memories ADD CONSTRAINT memories_id_vault_unique UNIQUE (id, vault_id);

ALTER TABLE extraction_queue
  ADD CONSTRAINT extraction_queue_chunk_vault_fkey
  FOREIGN KEY (chunk_id, vault_id) REFERENCES raw_chunks(id, vault_id);
ALTER TABLE extraction_queue
  ADD CONSTRAINT extraction_queue_segment_vault_fkey
  FOREIGN KEY (segment_id, vault_id) REFERENCES segments(id, vault_id) ON DELETE CASCADE;

ALTER TABLE curation_queue
  ADD CONSTRAINT curation_queue_segment_vault_fkey
  FOREIGN KEY (segment_id, vault_id) REFERENCES segments(id, vault_id) ON DELETE CASCADE;

ALTER TABLE memories
  ADD CONSTRAINT memories_parent_vault_fkey
  FOREIGN KEY (parent_id, vault_id) REFERENCES memories(id, vault_id) ON DELETE SET NULL (parent_id);
ALTER TABLE memories
  ADD CONSTRAINT memories_source_segment_vault_fkey
  FOREIGN KEY (source_segment_id, vault_id) REFERENCES segments(id, vault_id) ON DELETE SET NULL (source_segment_id);

ALTER TABLE memory_edges
  ADD CONSTRAINT memory_edges_from_vault_fkey
  FOREIGN KEY (from_memory_id, vault_id) REFERENCES memories(id, vault_id) ON DELETE CASCADE;
ALTER TABLE memory_edges
  ADD CONSTRAINT memory_edges_to_vault_fkey
  FOREIGN KEY (to_memory_id, vault_id) REFERENCES memories(id, vault_id) ON DELETE CASCADE;

ALTER TABLE contradiction_scan_log
  ADD CONSTRAINT contradiction_scan_memory_a_vault_fkey
  FOREIGN KEY (memory_id_a, vault_id) REFERENCES memories(id, vault_id) ON DELETE CASCADE;
ALTER TABLE contradiction_scan_log
  ADD CONSTRAINT contradiction_scan_memory_b_vault_fkey
  FOREIGN KEY (memory_id_b, vault_id) REFERENCES memories(id, vault_id) ON DELETE CASCADE;

ALTER TABLE curation_action_log
  ADD CONSTRAINT curation_action_segment_vault_fkey
  FOREIGN KEY (segment_id, vault_id) REFERENCES segments(id, vault_id) ON DELETE CASCADE;
ALTER TABLE curation_action_log
  ADD CONSTRAINT curation_action_memory_vault_fkey
  FOREIGN KEY (memory_id, vault_id) REFERENCES memories(id, vault_id);
ALTER TABLE curation_action_log
  ADD CONSTRAINT curation_action_new_memory_vault_fkey
  FOREIGN KEY (new_memory_id, vault_id) REFERENCES memories(id, vault_id);

ALTER TABLE curation_review_runs
  ADD CONSTRAINT curation_review_segment_vault_fkey
  FOREIGN KEY (segment_id, vault_id) REFERENCES segments(id, vault_id) ON DELETE CASCADE;
ALTER TABLE curation_dead_letter
  ADD CONSTRAINT curation_dead_letter_segment_vault_fkey
  FOREIGN KEY (segment_id, vault_id) REFERENCES segments(id, vault_id) ON DELETE CASCADE;

CREATE OR REPLACE FUNCTION persistio_assert_memory_audit_vault()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM memories WHERE id = NEW.memory_id AND vault_id = NEW.vault_id
  ) THEN
    RAISE EXCEPTION 'memory audit target must belong to vault %', NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS memory_scope_change_log_vault_guard ON memory_scope_change_log;
CREATE TRIGGER memory_scope_change_log_vault_guard
BEFORE INSERT ON memory_scope_change_log
FOR EACH ROW EXECUTE FUNCTION persistio_assert_memory_audit_vault();

DROP TRIGGER IF EXISTS memory_authority_events_vault_guard ON memory_authority_events;
CREATE TRIGGER memory_authority_events_vault_guard
BEFORE INSERT ON memory_authority_events
FOR EACH ROW EXECUTE FUNCTION persistio_assert_memory_audit_vault();

CREATE OR REPLACE FUNCTION persistio_assert_segment_chunk_vault()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM unnest(NEW.chunk_ids) AS requested(id)
    LEFT JOIN raw_chunks rc ON rc.id = requested.id AND rc.vault_id = NEW.vault_id
    WHERE rc.id IS NULL
  ) THEN
    RAISE EXCEPTION 'segment chunk_ids must all belong to vault %', NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM segments s
    CROSS JOIN LATERAL unnest(s.chunk_ids) AS requested(id)
    LEFT JOIN raw_chunks rc ON rc.id = requested.id AND rc.vault_id = s.vault_id
    WHERE rc.id IS NULL
  ) THEN
    RAISE EXCEPTION 'existing segment chunk_ids contain missing or cross-vault references';
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS segments_chunk_vault_guard ON segments;
CREATE CONSTRAINT TRIGGER segments_chunk_vault_guard
AFTER INSERT OR UPDATE OF vault_id, chunk_ids ON segments
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE FUNCTION persistio_assert_segment_chunk_vault();

CREATE OR REPLACE FUNCTION persistio_assert_memory_source_chunk_vault()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM unnest(NEW.source_chunks) AS requested(id)
    LEFT JOIN raw_chunks rc ON rc.id = requested.id AND rc.vault_id = NEW.vault_id
    WHERE rc.id IS NULL
  ) THEN
    RAISE EXCEPTION 'memory source_chunks must all belong to vault %', NEW.vault_id
      USING ERRCODE = '23503';
  END IF;
  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM memories m
    CROSS JOIN LATERAL unnest(m.source_chunks) AS requested(id)
    LEFT JOIN raw_chunks rc ON rc.id = requested.id AND rc.vault_id = m.vault_id
    WHERE rc.id IS NULL
  ) THEN
    RAISE EXCEPTION 'existing memory source_chunks contain missing or cross-vault references';
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS memories_source_chunk_vault_guard ON memories;
CREATE CONSTRAINT TRIGGER memories_source_chunk_vault_guard
AFTER INSERT OR UPDATE OF vault_id, source_chunks ON memories
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE FUNCTION persistio_assert_memory_source_chunk_vault();
