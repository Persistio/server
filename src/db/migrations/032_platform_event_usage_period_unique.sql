CREATE UNIQUE INDEX IF NOT EXISTS idx_platform_event_outbox_usage_period_closed_unique
  ON platform_event_outbox (event_type, subject, (payload->>'period'))
  WHERE event_type = 'vault.usage_period.closed';
