UPDATE plans
SET limits = limits || '{"graphEnabled": true}'::jsonb
WHERE id = 'unlimited';
