CREATE TABLE IF NOT EXISTS platform_event_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  schema_version INT NOT NULL DEFAULT 1,
  occurred_at TIMESTAMPTZ NOT NULL,
  subject TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_attempted_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT platform_event_outbox_status_check
    CHECK (status IN ('pending', 'delivered', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_platform_event_outbox_status_created_at
  ON platform_event_outbox (status, created_at);
