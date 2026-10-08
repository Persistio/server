-- Dormant admission metadata. Existing business leases/fences remain authoritative.
CREATE TABLE worker_execution_slot (
  id integer PRIMARY KEY CHECK (id = 1),
  owner uuid,
  kind text,
  token uuid,
  generation bigint,
  expires_at timestamptz,
  runtime_state jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(runtime_state) = 'object'),
  CHECK (
    (owner IS NULL AND kind IS NULL AND token IS NULL AND generation IS NULL AND expires_at IS NULL)
    OR
    (owner IS NOT NULL AND kind IS NOT NULL AND token IS NOT NULL AND generation IS NOT NULL
      AND generation > 0 AND expires_at IS NOT NULL)
  )
);
INSERT INTO worker_execution_slot(id) VALUES(1);
