-- Additive, dormant outbox. No triggers or changes to business tables/old writers.
CREATE TABLE worker_wake_intents (
  kind text PRIMARY KEY CHECK (kind IN (
    'extraction', 'curation', 'contradiction', 'platform_outbox', 'usage_periods', 'blob_reconciliation'
  )),
  requested_generation bigint NOT NULL DEFAULT 0 CHECK (requested_generation >= 0),
  completed_generation bigint NOT NULL DEFAULT 0 CHECK (completed_generation >= 0),
  pending_due_at timestamptz,
  active_generation bigint,
  active_token uuid,
  active_due_at timestamptz,
  dispatched boolean NOT NULL DEFAULT false,
  dispatch_owner uuid,
  dispatch_expires_at timestamptz,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 20),
  cycle_interval_ms integer NOT NULL DEFAULT 0 CHECK (cycle_interval_ms BETWEEN 0 AND 86400000),
  allowance integer NOT NULL DEFAULT 0 CHECK (allowance >= 0),
  allowance_used integer NOT NULL DEFAULT 0 CHECK (allowance_used BETWEEN 0 AND allowance),
  next_cycle_at timestamptz NOT NULL DEFAULT now(),
  CHECK (completed_generation <= requested_generation),
  CHECK (
    (active_generation IS NULL AND active_token IS NULL AND active_due_at IS NULL
      AND NOT dispatched AND dispatch_owner IS NULL AND dispatch_expires_at IS NULL
      AND allowance = 0 AND allowance_used = 0 AND cycle_interval_ms = 0)
    OR
    (active_generation IS NOT NULL AND active_generation > completed_generation AND active_generation <= requested_generation
      AND active_token IS NOT NULL AND active_due_at IS NOT NULL)
  ),
  CHECK ((dispatch_owner IS NULL) = (dispatch_expires_at IS NULL)),
  CHECK ((pending_due_at IS NOT NULL) =
    (requested_generation > COALESCE(active_generation, completed_generation)))
);
