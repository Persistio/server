-- Dedicated destination ownership: existing shadow rows can never reach serving totals.
-- Keep this table and its delivery runtime during worker rollback.
CREATE TABLE customer_metric_outbox (LIKE customer_metric_shadow_outbox INCLUDING ALL);
