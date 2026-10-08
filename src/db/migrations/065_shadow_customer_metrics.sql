-- Dormant, additive shadow transport. No foreign keys: deletion must not erase
-- already committed customer activity. Never connect this stream to serving totals.
CREATE TABLE customer_metric_shadow_outbox (
  event_id uuid PRIMARY KEY,
  event_ts timestamptz NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 16384),
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'dead')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 10),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_owner uuid,
  lease_expires_at timestamptz,
  settled_at timestamptz,
  CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
  CHECK ((status = 'pending') = (settled_at IS NULL)),
  CHECK (status = 'pending' OR lease_owner IS NULL),
  CHECK (payload->>'event_id' = event_id::text),
  CHECK (payload->>'contract_version' = '1')
);
CREATE INDEX customer_metric_shadow_due ON customer_metric_shadow_outbox(available_at, event_id) WHERE status='pending';
CREATE INDEX customer_metric_shadow_retention ON customer_metric_shadow_outbox(settled_at) WHERE status<>'pending';
CREATE TABLE worker_observation_success (
  kind text PRIMARY KEY CHECK (kind IN ('extraction','curation','contradiction','platform_outbox','usage_periods','blob_reconciliation')),
  succeeded_at timestamptz NOT NULL
);
