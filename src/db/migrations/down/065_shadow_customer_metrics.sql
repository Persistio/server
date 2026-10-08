-- Explicitly refuse rollback while undelivered evidence remains.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM customer_metric_shadow_outbox WHERE status <> 'delivered') THEN
    RAISE EXCEPTION 'Review/export pending and dead shadow events before rollback';
  END IF;
END $$;
DROP TABLE customer_metric_shadow_outbox;
DROP TABLE worker_observation_success;
