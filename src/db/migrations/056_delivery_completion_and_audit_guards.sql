-- Drain recall/ACK writers before upgrading. Retained rows are never rewritten.
LOCK TABLE memory_delivery_runs, memory_delivery_events IN ACCESS EXCLUSIVE MODE;
ALTER TABLE memory_delivery_runs ADD COLUMN completion_protocol SMALLINT NOT NULL DEFAULT 0
  CHECK (completion_protocol IN (0, 1));
ALTER TABLE memory_delivery_runs ALTER COLUMN completion_protocol SET DEFAULT 1;

CREATE FUNCTION persistio_delivery_snapshot(e memory_delivery_events) RETURNS JSONB
LANGUAGE sql IMMUTABLE AS $$
  SELECT to_jsonb(e) - ARRAY['id','stage','drop_reason','render_target','token_budget',
    'rendered_tokens','truncated','occurred_at']::text[];
$$;

-- Single evidence classifier for migration, ACK validation and operator audit.
-- Never consult mutable memories: these are the snapshots prepared for handoff.
CREATE FUNCTION persistio_delivery_integrity(run_id UUID, run_vault UUID)
RETURNS TABLE(selection_valid BOOLEAN, terminal_count INTEGER, terminal_valid BOOLEAN,
              outcome JSONB, last_terminal_at TIMESTAMPTZ)
LANGUAGE sql STABLE AS $$
  WITH run AS (
    SELECT * FROM memory_delivery_runs WHERE id=run_id AND vault_id=run_vault
  ), events AS MATERIALIZED (
    SELECT * FROM memory_delivery_events WHERE delivery_id=run_id AND vault_id=run_vault
  ), selected AS (SELECT * FROM events WHERE stage='selected'),
  returned AS (SELECT * FROM events WHERE stage='returned'),
  terminal AS (SELECT * FROM events WHERE stage IN ('rendered','dropped')),
  stats AS (
    SELECT count(*)::int AS n,
      count(DISTINCT jsonb_build_array(token_budget,rendered_tokens,truncated,render_target)) AS meta_count,
      bool_and(token_budget >= 0 AND rendered_tokens >= 0 AND rendered_tokens <= token_budget) AS tokens_valid,
      max(occurred_at) AS last_at,
      jsonb_build_object(
        'rendered_ids', COALESCE(jsonb_agg(memory_id::text ORDER BY memory_id) FILTER (WHERE stage='rendered'),'[]'::jsonb),
        'dropped', COALESCE(jsonb_agg(jsonb_build_object('id',memory_id::text,'reason',drop_reason) ORDER BY memory_id) FILTER (WHERE stage='dropped'),'[]'::jsonb),
        'token_budget', min(token_budget), 'rendered_tokens', min(rendered_tokens),
        'truncated', bool_or(truncated), 'render_target', min(render_target)
      ) AS canonical FROM terminal
  ), validity AS (
    SELECT run.selected_count BETWEEN 0 AND 100
      AND run.selected_count=(SELECT count(*) FROM selected)
      AND run.selected_count=(SELECT count(*) FROM returned)
      AND run.global_selected_count=(SELECT count(*) FROM selected WHERE memory_type='user_rule' AND scope='global')
      AND NOT EXISTS (
        SELECT 1 FROM selected s FULL JOIN returned r USING(memory_id)
        WHERE s.memory_id IS NULL OR r.memory_id IS NULL
          OR persistio_delivery_snapshot(s) IS DISTINCT FROM persistio_delivery_snapshot(r)
      ) AS valid, run.selected_count FROM run
  )
  SELECT validity.valid, stats.n,
    validity.valid AND stats.n=validity.selected_count AND stats.n>0
      AND stats.meta_count=1 AND stats.tokens_valid
      AND NOT EXISTS (
        SELECT 1 FROM selected s FULL JOIN terminal t USING(memory_id)
        WHERE s.memory_id IS NULL OR t.memory_id IS NULL
          OR persistio_delivery_snapshot(s) IS DISTINCT FROM persistio_delivery_snapshot(t)
      ),
    stats.canonical, stats.last_at FROM validity CROSS JOIN stats;
$$;

CREATE TABLE memory_delivery_acknowledgements (
  delivery_id UUID PRIMARY KEY,
  vault_id UUID NOT NULL,
  outcome JSONB NOT NULL CHECK (jsonb_typeof(outcome)='object'),
  outcome_hash TEXT GENERATED ALWAYS AS (encode(digest(outcome::text,'sha256'),'hex')) STORED,
  token_budget INTEGER GENERATED ALWAYS AS ((outcome->>'token_budget')::integer) STORED NOT NULL,
  rendered_tokens INTEGER GENERATED ALWAYS AS ((outcome->>'rendered_tokens')::integer) STORED NOT NULL,
  truncated BOOLEAN GENERATED ALWAYS AS ((outcome->>'truncated')::boolean) STORED NOT NULL,
  render_target TEXT GENERATED ALWAYS AS (outcome->>'render_target') STORED NOT NULL,
  origin TEXT NOT NULL CHECK (origin IN ('client_ack','legacy_terminal_events')),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  original_terminal_at TIMESTAMPTZ,
  FOREIGN KEY(delivery_id,vault_id) REFERENCES memory_delivery_runs(id,vault_id) ON DELETE RESTRICT,
  CHECK (token_budget>=0 AND rendered_tokens>=0 AND rendered_tokens<=token_budget),
  CHECK (render_target IN ('prompt_context','tool_response')),
  CHECK ((origin='client_ack' AND original_terminal_at IS NULL)
    OR (origin='legacy_terminal_events' AND original_terminal_at IS NOT NULL))
);

CREATE FUNCTION persistio_guard_delivery_ack() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE evidence RECORD; protocol SMALLINT;
BEGIN
  SELECT completion_protocol INTO protocol FROM memory_delivery_runs
    WHERE id=NEW.delivery_id AND vault_id=NEW.vault_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'delivery not found' USING ERRCODE='23503'; END IF;
  SELECT * INTO evidence FROM persistio_delivery_integrity(NEW.delivery_id,NEW.vault_id);
  IF evidence.selection_valid IS NOT TRUE OR
     (evidence.terminal_count>0 AND (evidence.terminal_valid IS NOT TRUE OR NEW.outcome IS DISTINCT FROM evidence.outcome)) OR
     (evidence.terminal_count=0 AND NEW.outcome IS DISTINCT FROM jsonb_build_object(
        'rendered_ids','[]'::jsonb,'dropped','[]'::jsonb,
        'token_budget',NEW.outcome->'token_budget','rendered_tokens',NEW.outcome->'rendered_tokens',
        'truncated',NEW.outcome->'truncated','render_target',NEW.outcome->'render_target')) OR
     (evidence.terminal_count=0 AND EXISTS(SELECT 1 FROM memory_delivery_events WHERE delivery_id=NEW.delivery_id)) OR
     jsonb_typeof(NEW.outcome->'token_budget') IS DISTINCT FROM 'number' OR
     jsonb_typeof(NEW.outcome->'rendered_tokens') IS DISTINCT FROM 'number' OR
     jsonb_typeof(NEW.outcome->'truncated') IS DISTINCT FROM 'boolean' OR
     (NEW.outcome->>'token_budget')::numeric <> trunc((NEW.outcome->>'token_budget')::numeric) OR
     (NEW.outcome->>'rendered_tokens')::numeric <> trunc((NEW.outcome->>'rendered_tokens')::numeric) OR
     (NEW.origin='legacy_terminal_events' AND (protocol<>0 OR evidence.terminal_valid IS NOT TRUE
        OR NEW.original_terminal_at IS DISTINCT FROM evidence.last_terminal_at))
  THEN RAISE EXCEPTION 'delivery acknowledgement does not match immutable evidence' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER memory_delivery_ack_reference_guard BEFORE INSERT ON memory_delivery_acknowledgements
  FOR EACH ROW EXECUTE FUNCTION persistio_guard_delivery_ack();
CREATE TRIGGER memory_delivery_ack_append_only BEFORE UPDATE OR DELETE ON memory_delivery_acknowledgements
  FOR EACH ROW EXECUTE FUNCTION reject_memory_observability_mutation();

-- Reconstruct only outcomes already proved by complete, consistent old events.
INSERT INTO memory_delivery_acknowledgements(delivery_id,vault_id,outcome,origin,original_terminal_at)
SELECT r.id,r.vault_id,i.outcome,'legacy_terminal_events',i.last_terminal_at
FROM memory_delivery_runs r CROSS JOIN LATERAL persistio_delivery_integrity(r.id,r.vault_id) i
WHERE i.terminal_valid;

CREATE TABLE memory_delivery_pending (
  delivery_id UUID PRIMARY KEY,
  vault_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  integrity_error BOOLEAN NOT NULL,
  FOREIGN KEY(delivery_id,vault_id) REFERENCES memory_delivery_runs(id,vault_id) ON DELETE RESTRICT
);
CREATE INDEX idx_memory_delivery_pending_age ON memory_delivery_pending(created_at,delivery_id);
CREATE INDEX idx_memory_delivery_pending_invalid ON memory_delivery_pending(delivery_id) WHERE integrity_error;

INSERT INTO memory_delivery_pending
SELECT r.id,r.vault_id,r.created_at,NOT i.selection_valid OR (i.terminal_count>0 AND NOT i.terminal_valid)
FROM memory_delivery_runs r CROSS JOIN LATERAL persistio_delivery_integrity(r.id,r.vault_id) i
WHERE NOT EXISTS(SELECT 1 FROM memory_delivery_acknowledgements a WHERE a.delivery_id=r.id)
  AND NOT (r.selected_count=0 AND i.selection_valid AND i.terminal_count=0);

CREATE FUNCTION persistio_guard_delivery_pending() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r memory_delivery_runs; evidence RECORD;
BEGIN
  IF TG_OP='DELETE' THEN
    IF NOT EXISTS(SELECT 1 FROM memory_delivery_acknowledgements WHERE delivery_id=OLD.delivery_id AND vault_id=OLD.vault_id)
    THEN RAISE EXCEPTION 'pending delivery requires an acknowledgement before removal'; END IF;
    RETURN OLD;
  END IF;
  SELECT * INTO r FROM memory_delivery_runs WHERE id=NEW.delivery_id AND vault_id=NEW.vault_id FOR UPDATE;
  IF NOT FOUND OR r.created_at IS DISTINCT FROM NEW.created_at OR
    (TG_OP='UPDATE' AND (OLD.delivery_id IS DISTINCT FROM NEW.delivery_id OR OLD.vault_id IS DISTINCT FROM NEW.vault_id)) OR
    EXISTS(SELECT 1 FROM memory_delivery_acknowledgements WHERE delivery_id=NEW.delivery_id)
  THEN RAISE EXCEPTION 'invalid pending delivery identity'; END IF;
  SELECT * INTO evidence FROM persistio_delivery_integrity(NEW.delivery_id,NEW.vault_id);
  IF NEW.integrity_error IS DISTINCT FROM (NOT evidence.selection_valid OR (evidence.terminal_count>0 AND NOT evidence.terminal_valid))
  THEN RAISE EXCEPTION 'invalid pending delivery classification'; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER memory_delivery_pending_guard BEFORE INSERT OR UPDATE OR DELETE ON memory_delivery_pending
  FOR EACH ROW EXECUTE FUNCTION persistio_guard_delivery_pending();

CREATE FUNCTION persistio_start_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.completion_protocol<>1 OR NEW.selected_count>100 THEN
    RAISE EXCEPTION 'new delivery requires current completion protocol and bounded selection';
  END IF;
  INSERT INTO memory_delivery_pending VALUES(NEW.id,NEW.vault_id,NEW.created_at,NEW.selected_count<>0);
  RETURN NEW;
END;
$$;
CREATE TRIGGER memory_delivery_start AFTER INSERT ON memory_delivery_runs
  FOR EACH ROW EXECUTE FUNCTION persistio_start_delivery();

CREATE FUNCTION persistio_guard_delivery_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE selected memory_delivery_events; max_count INTEGER;
BEGIN
  SELECT selected_count INTO max_count FROM memory_delivery_runs
    WHERE id=NEW.delivery_id AND vault_id=NEW.vault_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'delivery not found' USING ERRCODE='23503'; END IF;
  IF EXISTS(SELECT 1 FROM memory_delivery_acknowledgements WHERE delivery_id=NEW.delivery_id) THEN
    RAISE EXCEPTION 'acknowledged delivery cannot acquire more events';
  END IF;
  IF NEW.stage='selected' THEN
    IF (SELECT count(*) FROM memory_delivery_events WHERE delivery_id=NEW.delivery_id AND stage='selected') >= max_count THEN
      RAISE EXCEPTION 'delivery selection exceeds recorded count';
    END IF;
  ELSE
    SELECT * INTO selected FROM memory_delivery_events
      WHERE delivery_id=NEW.delivery_id AND vault_id=NEW.vault_id AND memory_id=NEW.memory_id AND stage='selected';
    IF NOT FOUND OR persistio_delivery_snapshot(selected) IS DISTINCT FROM persistio_delivery_snapshot(NEW) THEN
      RAISE EXCEPTION 'delivery event differs from selected snapshot';
    END IF;
    IF NEW.stage IN ('rendered','dropped') AND NOT EXISTS(
      SELECT 1 FROM memory_delivery_events WHERE delivery_id=NEW.delivery_id AND memory_id=NEW.memory_id AND stage='returned'
    ) THEN RAISE EXCEPTION 'terminal event requires returned evidence'; END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER memory_delivery_event_guard BEFORE INSERT ON memory_delivery_events
  FOR EACH ROW EXECUTE FUNCTION persistio_guard_delivery_event();

CREATE FUNCTION persistio_refresh_delivery_pending() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected RECORD; evidence RECORD;
BEGIN
  FOR affected IN SELECT DISTINCT delivery_id,vault_id FROM inserted_events ORDER BY delivery_id LOOP
    SELECT * INTO evidence FROM persistio_delivery_integrity(affected.delivery_id,affected.vault_id);
    UPDATE memory_delivery_pending SET integrity_error=NOT evidence.selection_valid OR (evidence.terminal_count>0 AND NOT evidence.terminal_valid)
      WHERE delivery_id=affected.delivery_id;
  END LOOP;
  RETURN NULL;
END;
$$;
CREATE TRIGGER memory_delivery_refresh_pending AFTER INSERT ON memory_delivery_events
  REFERENCING NEW TABLE AS inserted_events FOR EACH STATEMENT EXECUTE FUNCTION persistio_refresh_delivery_pending();

CREATE FUNCTION persistio_complete_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM memory_delivery_pending WHERE delivery_id=NEW.delivery_id AND vault_id=NEW.vault_id;
  RETURN NEW;
END;
$$;
CREATE TRIGGER memory_delivery_complete AFTER INSERT ON memory_delivery_acknowledgements
  FOR EACH ROW EXECUTE FUNCTION persistio_complete_delivery();

DO $$ DECLARE protected_table TEXT;
BEGIN
  FOREACH protected_table IN ARRAY ARRAY[
    'memory_mutation_events','memory_delivery_runs','memory_delivery_events',
    'memory_authority_events','memory_scope_change_log','contradiction_scan_log',
    'curation_action_log','curation_review_runs','curation_dead_letter','extraction_dead_letter',
    'memory_delivery_acknowledgements','memory_delivery_pending','memories'
  ] LOOP
    EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_memory_observability_mutation()',
      protected_table || '_truncate_protected',protected_table);
  END LOOP;
END $$;
