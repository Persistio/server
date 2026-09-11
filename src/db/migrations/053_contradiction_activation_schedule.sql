-- Policy-specific durable reminders. Neither readiness nor completion under one
-- policy is allowed to consume a different policy's outstanding work.
CREATE FUNCTION contradiction_authority_eligible(memory_row memories, global_policy text) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT (
    (
      (memory_row).type IS NOT DISTINCT FROM 'user_rule'
      AND (memory_row).scope IS NOT DISTINCT FROM 'global'
      AND (
        (global_policy::text = 'legacy'
          AND NOT EXISTS (
            SELECT 1
            FROM memory_authority_events revocation_event
            WHERE revocation_event.vault_id = (memory_row).vault_id
              AND revocation_event.memory_id = (memory_row).id
              AND revocation_event.event_type = 'revoke'
              AND NOT EXISTS (
                SELECT 1
                FROM memory_authority_events later_approval
                WHERE later_approval.vault_id = (memory_row).vault_id
                  AND later_approval.memory_id = (memory_row).id
                  AND later_approval.event_type = 'approve'
                  AND later_approval.new_state = 'approved'
                  AND later_approval.new_version > revocation_event.new_version
                  AND later_approval.new_version <= (memory_row).authority_version
              )
          )
          AND (
            ((memory_row).authority_state = 'approved' AND EXISTS (
              SELECT 1
              FROM memory_authority_events authority_event
              WHERE authority_event.vault_id = (memory_row).vault_id
                AND authority_event.memory_id = (memory_row).id
                AND authority_event.event_type = 'approve'
                AND authority_event.new_state = 'approved'
                AND authority_event.new_version = (memory_row).authority_version
            ))
            OR ((memory_row).authority_state = 'proposed' AND EXISTS (
              SELECT 1
              FROM memory_authority_events migration_event
              WHERE migration_event.vault_id = (memory_row).vault_id
                AND migration_event.memory_id = (memory_row).id
                AND migration_event.event_type = 'migration'
                AND migration_event.source = 'migration'
                AND migration_event.actor_type = 'system'
                AND migration_event.new_state = 'proposed'
                AND migration_event.new_version = (memory_row).authority_version
                AND migration_event.snapshot IS NOT NULL
                AND migration_event.snapshot->>'type' = 'user_rule'
                AND migration_event.snapshot->>'scope' = 'global'
                AND migration_event.snapshot->>'status' = 'active'
                AND migration_event.snapshot->>'archived_at' IS NULL
            ))
          )
        )
        OR (global_policy::text = 'approved_only'
          AND NOT EXISTS (
            SELECT 1
            FROM memory_authority_events revocation_event
            WHERE revocation_event.vault_id = (memory_row).vault_id
              AND revocation_event.memory_id = (memory_row).id
              AND revocation_event.event_type = 'revoke'
              AND NOT EXISTS (
                SELECT 1
                FROM memory_authority_events later_approval
                WHERE later_approval.vault_id = (memory_row).vault_id
                  AND later_approval.memory_id = (memory_row).id
                  AND later_approval.event_type = 'approve'
                  AND later_approval.new_state = 'approved'
                  AND later_approval.new_version > revocation_event.new_version
                  AND later_approval.new_version <= (memory_row).authority_version
              )
          )
          AND (memory_row).authority_state = 'approved' AND EXISTS (
            SELECT 1
            FROM memory_authority_events authority_event
            WHERE authority_event.vault_id = (memory_row).vault_id
              AND authority_event.memory_id = (memory_row).id
              AND authority_event.event_type = 'approve'
              AND authority_event.new_state = 'approved'
              AND authority_event.new_version = (memory_row).authority_version
          ))
      )
    )
    OR (
      NOT (
        (memory_row).type IS NOT DISTINCT FROM 'user_rule'
        AND (memory_row).scope IS NOT DISTINCT FROM 'global'
      )
      AND (
        NOT (memory_row).authority_required
        OR ((memory_row).authority_state = 'approved' AND EXISTS (
          SELECT 1
          FROM memory_authority_events authority_event
          WHERE authority_event.vault_id = (memory_row).vault_id
            AND authority_event.memory_id = (memory_row).id
            AND authority_event.event_type = 'approve'
            AND authority_event.new_state = 'approved'
            AND authority_event.new_version = (memory_row).authority_version
        ))
      )
    )
  );
$$;

CREATE TABLE memory_contradiction_schedule (
  memory_id UUID NOT NULL,
  policy TEXT NOT NULL CHECK (policy IN ('off', 'approved_only', 'legacy')),
  vault_id UUID NOT NULL,
  generation UUID NOT NULL DEFAULT gen_random_uuid(),
  authority_ready BOOLEAN NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  failures INTEGER NOT NULL DEFAULT 0 CHECK (failures BETWEEN 0 AND 10),
  PRIMARY KEY (memory_id, policy),
  FOREIGN KEY (vault_id, memory_id) REFERENCES memories(vault_id, id) ON DELETE CASCADE
);
CREATE INDEX memory_contradiction_schedule_ready_due
  ON memory_contradiction_schedule (policy, vault_id, available_at, memory_id)
  WHERE authority_ready;

-- Counts include only authority-ready reminders, including future-dated ones.
-- Held work stays durable in the schedule without occupying runnable discovery.
CREATE TABLE memory_contradiction_pending_vaults (
  policy TEXT NOT NULL CHECK (policy IN ('off', 'approved_only', 'legacy')),
  vault_id UUID NOT NULL REFERENCES vaults(id) ON DELETE CASCADE,
  pending_count BIGINT NOT NULL CHECK (pending_count >= 0),
  revision BIGINT NOT NULL DEFAULT 0,
  next_visit_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (policy, vault_id)
);
CREATE INDEX memory_contradiction_pending_vaults_vault
  ON memory_contradiction_pending_vaults (vault_id, policy);
CREATE INDEX memory_contradiction_pending_vaults_due
  ON memory_contradiction_pending_vaults (policy, next_visit_at, vault_id)
  WHERE pending_count > 0;

CREATE FUNCTION maintain_contradiction_pending_vault() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF OLD.authority_ready AND NEW.authority_ready
      AND OLD.policy = NEW.policy AND OLD.vault_id = NEW.vault_id
    THEN
      UPDATE memory_contradiction_pending_vaults
         SET revision = revision + 1,
             next_visit_at = LEAST(next_visit_at, NEW.available_at)
       WHERE policy = NEW.policy AND vault_id = NEW.vault_id;
      RETURN NULL;
    END IF;
  END IF;

  IF TG_OP <> 'INSERT' THEN
    IF OLD.authority_ready THEN
      UPDATE memory_contradiction_pending_vaults
         SET pending_count = pending_count - 1, revision = revision + 1
       WHERE policy = OLD.policy AND vault_id = OLD.vault_id;
    END IF;
  END IF;

  IF TG_OP <> 'DELETE' THEN
    IF NEW.authority_ready THEN
      INSERT INTO memory_contradiction_pending_vaults
        (policy, vault_id, pending_count, revision, next_visit_at)
      VALUES (NEW.policy, NEW.vault_id, 1, 1, NEW.available_at)
      ON CONFLICT (policy, vault_id) DO UPDATE
        SET pending_count = memory_contradiction_pending_vaults.pending_count + 1,
            revision = memory_contradiction_pending_vaults.revision + 1,
            next_visit_at = CASE WHEN memory_contradiction_pending_vaults.pending_count = 0
              THEN EXCLUDED.next_visit_at
              ELSE LEAST(memory_contradiction_pending_vaults.next_visit_at, EXCLUDED.next_visit_at) END;
    END IF;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER memory_contradiction_schedule_pending_vault
AFTER INSERT OR UPDATE OR DELETE ON memory_contradiction_schedule
FOR EACH ROW EXECUTE FUNCTION maintain_contradiction_pending_vault();

CREATE FUNCTION refresh_memory_contradiction_schedule(target_vault_id uuid, target_memory_id uuid) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  current_memory memories%ROWTYPE;
  current_policy text;
  is_ready boolean;
  is_schedulable boolean;
BEGIN
  -- Serialize event reconciliation with decision row locks, while remaining
  -- compatible with KEY SHARE locks held by other rows referencing this memory.
  -- Always reconcile the final row, not an earlier deferred trigger's NEW image.
  SELECT m.* INTO current_memory FROM memories m
  WHERE m.vault_id = target_vault_id AND m.id = target_memory_id
  FOR NO KEY UPDATE;
  IF NOT FOUND THEN RETURN; END IF;

  is_schedulable :=
    current_memory.status = 'active' AND current_memory.archived_at IS NULL
    AND current_memory.sensitivity <> 'restricted'
    AND current_memory.confidence > 0 AND current_memory.confidence <= 1
    AND (current_memory.valid_from IS NULL OR current_memory.valid_until IS NULL
      OR current_memory.valid_from <= current_memory.valid_until)
    AND (current_memory.valid_until IS NULL OR current_memory.valid_until >= (now() AT TIME ZONE 'UTC')::date)
    AND ((current_memory.scope = 'global' AND current_memory.scope_key IS NULL)
      OR (current_memory.scope IN ('project', 'task', 'session')
        AND current_memory.scope_key IS NOT NULL AND btrim(current_memory.scope_key) <> ''))
    AND (CASE WHEN current_memory.evidence ? 'policy_rejections' THEN
      CASE WHEN jsonb_typeof(current_memory.evidence -> 'policy_rejections') = 'array'
        THEN jsonb_array_length(current_memory.evidence -> 'policy_rejections') = 0
        ELSE false END
      ELSE true END);

  -- Use the same fixed policy order for insertion, replacement and removal.
  FOREACH current_policy IN ARRAY ARRAY['approved_only', 'legacy', 'off'] LOOP
    IF is_schedulable THEN
      -- Separate SPI statement after the row lock: if we waited for another
      -- event reconciliation, observe its committed authority evidence now.
      SELECT contradiction_authority_eligible(current_memory, current_policy) INTO is_ready;
      INSERT INTO memory_contradiction_schedule
        (memory_id, policy, vault_id, generation, authority_ready, available_at)
      VALUES (current_memory.id, current_policy, current_memory.vault_id,
        gen_random_uuid(), is_ready, GREATEST(now(),
          current_memory.valid_from::timestamp AT TIME ZONE 'UTC',
          current_memory.source_timestamp - interval '5 minutes'))
      ON CONFLICT (memory_id, policy) DO UPDATE
        SET generation = EXCLUDED.generation, authority_ready = EXCLUDED.authority_ready,
            available_at = EXCLUDED.available_at, failures = 0;
    ELSE
      DELETE FROM memory_contradiction_schedule
      WHERE memory_id = target_memory_id AND vault_id = target_vault_id AND policy = current_policy;
    END IF;
  END LOOP;
END;
$$;

CREATE FUNCTION schedule_memory_contradiction_activation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND ROW(
    NEW.data, NEW.embedding, NEW.subject, NEW.subject_encrypted, NEW.subject_hmac, NEW.categories,
    NEW.source_chunks, NEW.source_segment_id, NEW.type, NEW.scope, NEW.scope_key, NEW.polarity,
    NEW.status, NEW.archived_at, NEW.sensitivity, NEW.confidence, NEW.evidence, NEW.source_timestamp,
    NEW.valid_from, NEW.valid_until, NEW.authority_state, NEW.authority_required, NEW.authority_version
  ) IS NOT DISTINCT FROM ROW(
    OLD.data, OLD.embedding, OLD.subject, OLD.subject_encrypted, OLD.subject_hmac, OLD.categories,
    OLD.source_chunks, OLD.source_segment_id, OLD.type, OLD.scope, OLD.scope_key, OLD.polarity,
    OLD.status, OLD.archived_at, OLD.sensitivity, OLD.confidence, OLD.evidence, OLD.source_timestamp,
    OLD.valid_from, OLD.valid_until, OLD.authority_state, OLD.authority_required, OLD.authority_version
  ) THEN
    RETURN NULL;
  END IF;
  PERFORM refresh_memory_contradiction_schedule(NEW.vault_id, NEW.id);
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER memories_schedule_contradiction_activation
AFTER INSERT OR UPDATE ON memories
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION schedule_memory_contradiction_activation();

CREATE FUNCTION schedule_authority_event_contradiction_activation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM refresh_memory_contradiction_schedule(NEW.vault_id, NEW.memory_id);
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER memory_authority_events_schedule_contradiction_activation
AFTER INSERT ON memory_authority_events
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION schedule_authority_event_contradiction_activation();

-- One upgrade pass. Reuse the same final-row reconciliation as live writes.
DO $$
DECLARE
  target record;
BEGIN
  FOR target IN
    SELECT vault_id, id FROM memories
    WHERE status = 'active' AND archived_at IS NULL
    ORDER BY vault_id, id
  LOOP
    PERFORM refresh_memory_contradiction_schedule(target.vault_id, target.id);
  END LOOP;
END;
$$;
