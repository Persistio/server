UPDATE plans
SET limits = limits || '{
  "curator_enabled": true,
  "curator_schedule_interval_minutes": 15,
  "curator_runs_per_month": 3000,
  "curator_jobs_per_run": 20,
  "curator_candidates_per_run": 400,
  "curator_candidates_per_call": 20,
  "curator_active_memories_per_call": 80,
  "curator_input_tokens_per_call": 12000,
  "curator_output_tokens_per_call": 2000,
  "curator_tokens_per_month": 25000000,
  "curator_requests_per_month": 6000,
  "curator_max_queue_age_hours": 24,
  "curator_backlog_limit": 10000,
  "curator_priority_weight": 3.0,
  "curator_overage_mode": "payg"
}'::jsonb
WHERE id = 'unlimited';

UPDATE vaults
SET plan_id = 'unlimited'
WHERE plan_id = 'pro';

DELETE FROM plans
WHERE id = 'pro';
