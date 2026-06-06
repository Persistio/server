ALTER TABLE platform_event_outbox
  ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

ALTER TABLE platform_event_outbox
  DROP CONSTRAINT IF EXISTS platform_event_outbox_status_check;

ALTER TABLE platform_event_outbox
  ADD CONSTRAINT platform_event_outbox_status_check
  CHECK (status IN ('pending', 'delivered', 'failed', 'dead'));

CREATE INDEX IF NOT EXISTS idx_platform_event_outbox_retry
  ON platform_event_outbox (status, next_retry_at, created_at)
  WHERE status IN ('pending', 'failed');
