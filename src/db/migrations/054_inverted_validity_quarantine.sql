-- Preserve rejected temporal evidence without admitting it to eligible states.
-- NOT VALID retains 049's legacy-row strategy; new/changed rows are checked.
ALTER TABLE memories DROP CONSTRAINT memories_validity_window_order;
ALTER TABLE memories ADD CONSTRAINT memories_validity_window_order CHECK (
  valid_from IS NULL OR valid_until IS NULL OR valid_from <= valid_until
  OR COALESCE(
    (status = 'needs_review'
      OR (status IN ('superseded', 'contradicted') AND archived_at IS NOT NULL))
    AND jsonb_typeof(evidence) = 'object'
    AND jsonb_typeof(evidence -> 'policy_rejections') = 'array'
    AND (evidence -> 'policy_rejections') @>
      '[{"code":"invalid_memory_validity_window","field":"valid_until","reason":"inverted"}]'::jsonb,
    false
  )
) NOT VALID;
