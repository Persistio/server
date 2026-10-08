UPDATE plans
SET limits = jsonb_set(limits, '{curator_output_tokens_per_call}', '8192'::jsonb, true)
WHERE id = 'unlimited'
  AND limits->>'curator_output_tokens_per_call' = '2000';
