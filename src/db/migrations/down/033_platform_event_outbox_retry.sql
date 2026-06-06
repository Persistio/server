DROP INDEX IF EXISTS idx_platform_event_outbox_retry;

ALTER TABLE platform_event_outbox
  DROP CONSTRAINT IF EXISTS platform_event_outbox_status_check;

UPDATE platform_event_outbox
  SET status = 'failed'
  WHERE status = 'dead';

ALTER TABLE platform_event_outbox
  ADD CONSTRAINT platform_event_outbox_status_check
  CHECK (status IN ('pending', 'delivered', 'failed'));

ALTER TABLE platform_event_outbox
  DROP COLUMN IF EXISTS next_retry_at;
