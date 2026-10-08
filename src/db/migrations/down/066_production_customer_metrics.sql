-- Runtime rollback retains this table and its observer. Explicit schema downgrade
-- must not erase even delivered receipts until their evidence has been reviewed.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM customer_metric_outbox) THEN
    RAISE EXCEPTION 'Retain/export production customer events before schema downgrade';
  END IF;
END $$;
DROP TABLE customer_metric_outbox;
