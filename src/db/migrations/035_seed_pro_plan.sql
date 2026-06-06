INSERT INTO plans (id, limits)
SELECT 'pro', limits
FROM plans
WHERE id = 'unlimited'
ON CONFLICT (id) DO NOTHING;
