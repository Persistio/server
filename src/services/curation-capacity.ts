import type { PoolClient, QueryResultRow } from 'pg';

import { query, withTransaction } from '../db/client';
import { getCurrentUsagePeriod, writeUsagePeriodClosedEventIfNeeded } from './usage';

export interface CuratorPlanLimits {
  curator_enabled: boolean;
  curator_schedule_interval_minutes: number;
  curator_runs_per_month: number;
  curator_jobs_per_run: number;
  curator_candidates_per_run: number;
  curator_candidates_per_call: number;
  curator_active_memories_per_call: number;
  curator_input_tokens_per_call: number;
  curator_output_tokens_per_call: number;
  curator_tokens_per_month: number;
  curator_requests_per_month: number;
  curator_max_queue_age_hours: number;
  curator_backlog_limit: number;
  curator_priority_weight: number;
  curator_overage_mode: 'defer' | 'disable' | 'payg' | 'upgrade_only';
}

export interface CuratorUsageSnapshot {
  curator_runs: number;
  curator_requests: number;
  curator_input_tokens: number;
  curator_output_tokens: number;
  curator_candidates_processed: number;
  curator_candidates_deferred: number;
}

export interface CuratorCapacityStatus {
  vault_id: string;
  plan: string;
  period: string;
  limits: CuratorPlanLimits;
  usage: CuratorUsageSnapshot;
  backlog: {
    pending_queue_rows: number;
    oldest_queue_age_seconds: number | null;
    backlog_limit: number;
    pressure: 'ok' | 'warning' | 'over_limit';
  };
  schedule: {
    last_run_at: string | null;
    next_run_at: string | null;
    eligible_now: boolean;
    defer_reason: string | null;
  };
  recent_runs: Array<{
    segment_id: string;
    triggered_at: string;
    actions: number;
    applied_actions: number;
    errors: number;
  }>;
}

export interface ClaimedCurationJobRow {
  queue_id: string;
  vault_id: string;
  segment_id: string;
}

interface VaultLimitsRow extends QueryResultRow {
  plan_id: string;
  limits: Record<string, unknown> | null;
  rate_limit_override: Record<string, unknown> | null;
}

interface StatusRow extends QueryResultRow {
  plan_id: string;
  limits: Record<string, unknown> | null;
  rate_limit_override: Record<string, unknown> | null;
  curator_runs: string;
  curator_requests: string;
  curator_input_tokens: string;
  curator_output_tokens: string;
  curator_candidates_processed: string;
  curator_candidates_deferred: string;
  pending_queue_rows: string;
  available_queue_rows: string;
  oldest_queue_age_seconds: string | null;
  next_queue_available_at: string | null;
  last_curator_run_at: string | null;
  next_curator_run_at: string | null;
  curator_claimed_until: string | null;
  last_curator_defer_reason: string | null;
}

interface RecentRunRow extends QueryResultRow {
  segment_id: string;
  triggered_at: string;
  actions: string;
  applied_actions: string;
  errors: string;
}

const disabledCuratorLimits: CuratorPlanLimits = {
  curator_enabled: false,
  curator_schedule_interval_minutes: 1440,
  curator_runs_per_month: 0,
  curator_jobs_per_run: 0,
  curator_candidates_per_run: 0,
  curator_candidates_per_call: 0,
  curator_active_memories_per_call: 0,
  curator_input_tokens_per_call: 0,
  curator_output_tokens_per_call: 0,
  curator_tokens_per_month: 0,
  curator_requests_per_month: 0,
  curator_max_queue_age_hours: 24,
  curator_backlog_limit: 100,
  curator_priority_weight: 0,
  curator_overage_mode: 'disable'
};

const unlimitedCuratorLimits: CuratorPlanLimits = {
  curator_enabled: true,
  curator_schedule_interval_minutes: 15,
  curator_runs_per_month: 3000,
  curator_jobs_per_run: 20,
  curator_candidates_per_run: 400,
  curator_candidates_per_call: 20,
  curator_active_memories_per_call: 80,
  curator_input_tokens_per_call: 12000,
  curator_output_tokens_per_call: 2000,
  curator_tokens_per_month: 25000000,
  curator_requests_per_month: 6000,
  curator_max_queue_age_hours: 24,
  curator_backlog_limit: 10000,
  curator_priority_weight: 3,
  curator_overage_mode: 'payg'
};

const defaultCuratorLimitsByPlan: Record<string, CuratorPlanLimits> = {
  unlimited: unlimitedCuratorLimits,
  free: disabledCuratorLimits
};

function readBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') return value.toLowerCase() === 'true';
  return fallback;
}

function readNumber(value: unknown, fallback: number): number {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readOverageMode(value: unknown, fallback: CuratorPlanLimits['curator_overage_mode']): CuratorPlanLimits['curator_overage_mode'] {
  return value === 'defer' || value === 'disable' || value === 'payg' || value === 'upgrade_only' ? value : fallback;
}

export function mergeCuratorLimits(
  planId: string,
  limits: Record<string, unknown> | null,
  override: Record<string, unknown> | null
): CuratorPlanLimits {
  const base = defaultCuratorLimitsByPlan[planId] ?? disabledCuratorLimits;
  const merged = { ...base, ...(limits ?? {}), ...(override ?? {}) };
  return {
    curator_enabled: readBoolean(merged.curator_enabled, base.curator_enabled),
    curator_schedule_interval_minutes: readNumber(merged.curator_schedule_interval_minutes, base.curator_schedule_interval_minutes),
    curator_runs_per_month: readNumber(merged.curator_runs_per_month, base.curator_runs_per_month),
    curator_jobs_per_run: readNumber(merged.curator_jobs_per_run, base.curator_jobs_per_run),
    curator_candidates_per_run: readNumber(merged.curator_candidates_per_run, base.curator_candidates_per_run),
    curator_candidates_per_call: readNumber(merged.curator_candidates_per_call, base.curator_candidates_per_call),
    curator_active_memories_per_call: readNumber(merged.curator_active_memories_per_call, base.curator_active_memories_per_call),
    curator_input_tokens_per_call: readNumber(merged.curator_input_tokens_per_call, base.curator_input_tokens_per_call),
    curator_output_tokens_per_call: readNumber(merged.curator_output_tokens_per_call, base.curator_output_tokens_per_call),
    curator_tokens_per_month: readNumber(merged.curator_tokens_per_month, base.curator_tokens_per_month),
    curator_requests_per_month: readNumber(merged.curator_requests_per_month, base.curator_requests_per_month),
    curator_max_queue_age_hours: readNumber(merged.curator_max_queue_age_hours, base.curator_max_queue_age_hours),
    curator_backlog_limit: readNumber(merged.curator_backlog_limit, base.curator_backlog_limit),
    curator_priority_weight: readNumber(merged.curator_priority_weight, base.curator_priority_weight),
    curator_overage_mode: readOverageMode(merged.curator_overage_mode, base.curator_overage_mode)
  };
}

export async function getCuratorLimits(vaultId: string, client?: PoolClient): Promise<CuratorPlanLimits> {
  const result = client
    ? await client.query<VaultLimitsRow>(
      `SELECT v.plan_id, p.limits, v.rate_limit_override
       FROM vaults AS v
       JOIN plans AS p
         ON p.id = v.plan_id
       WHERE v.id = $1
       LIMIT 1`,
      [vaultId]
    )
    : await query<VaultLimitsRow>(
    `SELECT v.plan_id, p.limits, v.rate_limit_override
     FROM vaults AS v
     JOIN plans AS p
       ON p.id = v.plan_id
     WHERE v.id = $1
     LIMIT 1`,
    [vaultId]
    );

  if (!result.rowCount) {
    throw new Error(`Vault ${vaultId} not found`);
  }

  const row = result.rows[0];
  return mergeCuratorLimits(row.plan_id, row.limits, row.rate_limit_override);
}

export async function isCuratorEnabled(vaultId: string, client?: PoolClient): Promise<boolean> {
  return (await getCuratorLimits(vaultId, client)).curator_enabled;
}

export function getCuratorPlanBlockReason(limits: CuratorPlanLimits): string | null {
  if (!limits.curator_enabled) return 'curator disabled by plan';
  if (limits.curator_jobs_per_run <= 0) return 'curator jobs per run limit exhausted';
  if (limits.curator_candidates_per_run <= 0 || limits.curator_candidates_per_call <= 0) return 'curator candidate limit exhausted';
  if (limits.curator_input_tokens_per_call <= 0) return 'curator input token limit exhausted';
  if (limits.curator_output_tokens_per_call <= 0) return 'curator output token limit exhausted';
  return null;
}

export async function claimEligibleCurationJobs(limit: number, workerId: string): Promise<ClaimedCurationJobRow[]> {
  const period = getCurrentUsagePeriod();
  const result = await query<ClaimedCurationJobRow>(
    `WITH eligible_vaults AS (
       SELECT
         v.id AS vault_id,
         GREATEST(0, COALESCE((v.rate_limit_override->>'curator_jobs_per_run'), (p.limits->>'curator_jobs_per_run'), '0')::int) AS jobs_per_run,
         COALESCE((v.rate_limit_override->>'curator_priority_weight'), (p.limits->>'curator_priority_weight'), '0')::numeric AS priority_weight,
         COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_runs ELSE 0 END, 0) AS runs_used,
         COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_requests ELSE 0 END, 0) AS requests_used,
         COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_input_tokens + vu.curator_output_tokens ELSE 0 END, 0) AS tokens_used,
         NULLIF(COALESCE((v.rate_limit_override->>'curator_runs_per_month'), (p.limits->>'curator_runs_per_month'), '0'), '')::int AS runs_limit,
         NULLIF(COALESCE((v.rate_limit_override->>'curator_requests_per_month'), (p.limits->>'curator_requests_per_month'), '0'), '')::int AS requests_limit,
         NULLIF(COALESCE((v.rate_limit_override->>'curator_tokens_per_month'), (p.limits->>'curator_tokens_per_month'), '0'), '')::int AS tokens_limit,
         vcs.curator_claimed_until
       FROM vaults v
       JOIN plans p
         ON p.id = v.plan_id
       LEFT JOIN vault_usage vu
         ON vu.vault_id = v.id
       LEFT JOIN vault_curation_state vcs
         ON vcs.vault_id = v.id
       WHERE COALESCE((v.rate_limit_override->>'curator_enabled')::boolean, (p.limits->>'curator_enabled')::boolean, false)
         AND GREATEST(0, COALESCE((v.rate_limit_override->>'curator_jobs_per_run'), (p.limits->>'curator_jobs_per_run'), '0')::int) > 0
         AND COALESCE((v.rate_limit_override->>'curator_candidates_per_run'), (p.limits->>'curator_candidates_per_run'), '0')::int > 0
         AND COALESCE((v.rate_limit_override->>'curator_candidates_per_call'), (p.limits->>'curator_candidates_per_call'), '0')::int > 0
         AND COALESCE((v.rate_limit_override->>'curator_input_tokens_per_call'), (p.limits->>'curator_input_tokens_per_call'), '0')::int > 0
         AND COALESCE((v.rate_limit_override->>'curator_output_tokens_per_call'), (p.limits->>'curator_output_tokens_per_call'), '0')::int > 0
         AND (vcs.next_curator_run_at IS NULL OR vcs.next_curator_run_at <= now())
         AND (vcs.curator_claimed_until IS NULL OR vcs.curator_claimed_until <= now())
         AND EXISTS (
           SELECT 1
           FROM curation_queue cq
           WHERE cq.vault_id = v.id
             AND cq.claimed_at IS NULL
             AND cq.available_at <= now()
         )
     ),
     claimable AS (
       SELECT cq.id AS queue_id, cq.vault_id, cq.segment_id, cq.enqueued_at, ev.priority_weight
       FROM eligible_vaults ev
       JOIN LATERAL (
         SELECT cq.id, cq.vault_id, cq.segment_id, cq.enqueued_at
         FROM curation_queue cq
         WHERE cq.vault_id = ev.vault_id
           AND cq.claimed_at IS NULL
           AND cq.available_at <= now()
         ORDER BY cq.enqueued_at ASC
         LIMIT LEAST(
           ev.jobs_per_run,
           CASE
             WHEN ev.runs_limit IS NULL OR ev.runs_used < ev.runs_limit THEN ev.jobs_per_run
             ELSE 0
           END,
           GREATEST(0, COALESCE(ev.requests_limit - ev.requests_used, ev.jobs_per_run))
         )
         FOR UPDATE SKIP LOCKED
       ) cq ON true
       WHERE ev.tokens_used < ev.tokens_limit
     ),
     selected AS (
       SELECT queue_id, vault_id, segment_id
       FROM claimable
       ORDER BY priority_weight DESC, enqueued_at ASC
       LIMIT $1
     ),
     reserved_vaults AS (
       INSERT INTO vault_curation_state (vault_id, curator_claimed_until, curator_claimed_by, updated_at)
       SELECT DISTINCT vault_id, now() + interval '10 minutes', $3, now()
       FROM selected
       ON CONFLICT (vault_id) DO UPDATE
       SET curator_claimed_until = EXCLUDED.curator_claimed_until,
           curator_claimed_by = EXCLUDED.curator_claimed_by,
           updated_at = now()
       WHERE (vault_curation_state.next_curator_run_at IS NULL OR vault_curation_state.next_curator_run_at <= now())
         AND (vault_curation_state.curator_claimed_until IS NULL OR vault_curation_state.curator_claimed_until <= now())
       RETURNING vault_id
     ),
     claimed AS (
       SELECT selected.queue_id, selected.vault_id, selected.segment_id
       FROM selected
       JOIN reserved_vaults
         ON reserved_vaults.vault_id = selected.vault_id
     )
     UPDATE curation_queue cq
     SET claimed_at = now(), claimed_by = $3
     FROM claimed
     WHERE cq.id = claimed.queue_id
     RETURNING claimed.queue_id, claimed.vault_id, claimed.segment_id`,
    [limit, period, workerId]
  );

  return result.rows;
}

export async function recordCuratorUsage(input: {
  vaultId: string;
  candidatesProcessed: number;
  countRun?: boolean;
  promptTokens: number;
  completionTokens: number;
  limits: CuratorPlanLimits;
}) {
  const period = getCurrentUsagePeriod();
  const curatorRuns = input.countRun === false ? 0 : 1;
  await withTransaction(async (client) => {
    await writeUsagePeriodClosedEventIfNeeded(client, input.vaultId, period);
    await client.query(
      `INSERT INTO vault_usage (
         vault_id, period, curator_runs, curator_requests, curator_input_tokens,
         curator_output_tokens, curator_candidates_processed, updated_at
       )
       VALUES ($1, $2, $3, 1, $4, $5, $6, now())
       ON CONFLICT (vault_id) DO UPDATE
       SET period = EXCLUDED.period,
           ingest_events = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.ingest_events ELSE 0 END,
           memory_adds = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.memory_adds ELSE 0 END,
           searches = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.searches ELSE 0 END,
           curator_runs = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_runs + EXCLUDED.curator_runs ELSE EXCLUDED.curator_runs END,
           curator_requests = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_requests + 1 ELSE 1 END,
           curator_input_tokens = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_input_tokens + EXCLUDED.curator_input_tokens ELSE EXCLUDED.curator_input_tokens END,
           curator_output_tokens = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_output_tokens + EXCLUDED.curator_output_tokens ELSE EXCLUDED.curator_output_tokens END,
           curator_candidates_processed = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_candidates_processed + EXCLUDED.curator_candidates_processed ELSE EXCLUDED.curator_candidates_processed END,
           curator_candidates_deferred = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_candidates_deferred ELSE 0 END,
           updated_at = now()`,
      [input.vaultId, period, curatorRuns, input.promptTokens, input.completionTokens, input.candidatesProcessed]
    );
  });

  if (curatorRuns > 0) {
    await query(
      `INSERT INTO vault_curation_state (vault_id, last_curator_run_at, next_curator_run_at, last_curator_defer_reason, updated_at)
       VALUES ($1, now(), now() + ($2::text || ' minutes')::interval, NULL, now())
       ON CONFLICT (vault_id) DO UPDATE
       SET last_curator_run_at = EXCLUDED.last_curator_run_at,
           next_curator_run_at = EXCLUDED.next_curator_run_at,
           last_curator_defer_reason = NULL,
           curator_claimed_until = NULL,
           curator_claimed_by = NULL,
           updated_at = now()`,
      [input.vaultId, input.limits.curator_schedule_interval_minutes]
    );
  }
}

export async function recordCuratorDeferral(input: {
  vaultId: string;
  candidatesDeferred?: number;
  reason: string;
  availableAt?: Date;
}) {
  const period = getCurrentUsagePeriod();
  const candidatesDeferred = input.candidatesDeferred ?? 0;
  await withTransaction(async (client) => {
    await writeUsagePeriodClosedEventIfNeeded(client, input.vaultId, period);
    await client.query(
      `INSERT INTO vault_usage (vault_id, period, curator_candidates_deferred, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (vault_id) DO UPDATE
       SET period = EXCLUDED.period,
           ingest_events = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.ingest_events ELSE 0 END,
           memory_adds = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.memory_adds ELSE 0 END,
           searches = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.searches ELSE 0 END,
           curator_runs = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_runs ELSE 0 END,
           curator_requests = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_requests ELSE 0 END,
           curator_input_tokens = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_input_tokens ELSE 0 END,
           curator_output_tokens = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_output_tokens ELSE 0 END,
           curator_candidates_processed = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_candidates_processed ELSE 0 END,
           curator_candidates_deferred = CASE
             WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_candidates_deferred + EXCLUDED.curator_candidates_deferred
             ELSE EXCLUDED.curator_candidates_deferred
           END,
           updated_at = now()`,
      [input.vaultId, period, candidatesDeferred]
    );
  });

  await query(
    `INSERT INTO vault_curation_state (vault_id, next_curator_run_at, last_curator_defer_reason, updated_at)
     VALUES ($1, $2, $3, now())
     ON CONFLICT (vault_id) DO UPDATE
     SET next_curator_run_at = COALESCE($2, vault_curation_state.next_curator_run_at),
         last_curator_defer_reason = EXCLUDED.last_curator_defer_reason,
         curator_claimed_until = NULL,
         curator_claimed_by = NULL,
         updated_at = now()`,
    [input.vaultId, input.availableAt?.toISOString() ?? null, input.reason]
  );
}

export async function getCurationStatus(vaultId: string): Promise<CuratorCapacityStatus> {
  const period = getCurrentUsagePeriod();
  const [result, recentRunsResult] = await Promise.all([
    query<StatusRow>(
    `SELECT
       v.plan_id,
       p.limits,
       v.rate_limit_override,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_runs ELSE 0 END, 0)::text AS curator_runs,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_requests ELSE 0 END, 0)::text AS curator_requests,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_input_tokens ELSE 0 END, 0)::text AS curator_input_tokens,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_output_tokens ELSE 0 END, 0)::text AS curator_output_tokens,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_candidates_processed ELSE 0 END, 0)::text AS curator_candidates_processed,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.curator_candidates_deferred ELSE 0 END, 0)::text AS curator_candidates_deferred,
       COALESCE(queue.pending_queue_rows, 0)::text AS pending_queue_rows,
       COALESCE(queue.available_queue_rows, 0)::text AS available_queue_rows,
       queue.oldest_queue_age_seconds::text AS oldest_queue_age_seconds,
       queue.next_queue_available_at::text AS next_queue_available_at,
       vcs.last_curator_run_at::text AS last_curator_run_at,
       vcs.next_curator_run_at::text AS next_curator_run_at,
       vcs.curator_claimed_until::text AS curator_claimed_until,
       vcs.last_curator_defer_reason
     FROM vaults v
     JOIN plans p
       ON p.id = v.plan_id
     LEFT JOIN vault_usage vu
       ON vu.vault_id = v.id
     LEFT JOIN vault_curation_state vcs
       ON vcs.vault_id = v.id
     LEFT JOIN LATERAL (
       SELECT
         COUNT(*)::int AS pending_queue_rows,
         COUNT(*) FILTER (WHERE available_at <= now())::int AS available_queue_rows,
         EXTRACT(EPOCH FROM now() - MIN(enqueued_at))::int AS oldest_queue_age_seconds,
         MIN(available_at) FILTER (WHERE available_at > now()) AS next_queue_available_at
       FROM curation_queue
       WHERE vault_id = v.id
         AND claimed_at IS NULL
     ) queue ON true
     WHERE v.id = $1
     LIMIT 1`,
    [vaultId, period]
    ),
    query<RecentRunRow>(
      `SELECT
         segment_id::text,
         MAX(triggered_at)::text AS triggered_at,
         COUNT(*)::text AS actions,
         COUNT(*) FILTER (WHERE applied_at IS NOT NULL)::text AS applied_actions,
         COUNT(*) FILTER (WHERE error IS NOT NULL)::text AS errors
       FROM curation_action_log
       WHERE vault_id = $1
       GROUP BY segment_id
       ORDER BY MAX(triggered_at) DESC
       LIMIT 10`,
      [vaultId]
    )
  ]);

  if (!result.rowCount) {
    throw new Error(`Vault ${vaultId} not found`);
  }

  const row = result.rows[0];
  const limits = mergeCuratorLimits(row.plan_id, row.limits, row.rate_limit_override);
  const usage = {
    curator_runs: Number(row.curator_runs),
    curator_requests: Number(row.curator_requests),
    curator_input_tokens: Number(row.curator_input_tokens),
    curator_output_tokens: Number(row.curator_output_tokens),
    curator_candidates_processed: Number(row.curator_candidates_processed),
    curator_candidates_deferred: Number(row.curator_candidates_deferred)
  };
  const pendingQueueRows = Number(row.pending_queue_rows);
  const availableQueueRows = Number(row.available_queue_rows);
  const oldestQueueAgeSeconds = row.oldest_queue_age_seconds === null ? null : Number(row.oldest_queue_age_seconds);
  const tokensUsed = usage.curator_input_tokens + usage.curator_output_tokens;
  const currentDeferReason = getCapacityDeferReason(limits, usage, row.next_curator_run_at, row.next_queue_available_at, row.curator_claimed_until);
  const nextRunAt = getNextRunAt(row.next_curator_run_at, row.next_queue_available_at, row.curator_claimed_until);

  return {
    vault_id: vaultId,
    plan: row.plan_id,
    period,
    limits,
    usage,
    backlog: {
      pending_queue_rows: pendingQueueRows,
      oldest_queue_age_seconds: oldestQueueAgeSeconds,
      backlog_limit: limits.curator_backlog_limit,
      pressure: pendingQueueRows >= limits.curator_backlog_limit
        ? 'over_limit'
        : pendingQueueRows >= Math.ceil(limits.curator_backlog_limit * 0.8) ? 'warning' : 'ok'
    },
    schedule: {
      last_run_at: row.last_curator_run_at,
      next_run_at: nextRunAt,
      eligible_now: !currentDeferReason && availableQueueRows > 0 && tokensUsed < limits.curator_tokens_per_month,
      defer_reason: currentDeferReason
    },
    recent_runs: recentRunsResult.rows.map((run) => ({
      segment_id: run.segment_id,
      triggered_at: run.triggered_at,
      actions: Number(run.actions),
      applied_actions: Number(run.applied_actions),
      errors: Number(run.errors)
    }))
  };
}

export async function releaseCuratorClaim(vaultId: string, workerId: string): Promise<void> {
  await query(
    `UPDATE vault_curation_state
     SET curator_claimed_until = NULL,
         curator_claimed_by = NULL,
         updated_at = now()
     WHERE vault_id = $1
       AND curator_claimed_by = $2`,
    [vaultId, workerId]
  );
}

function getNextRunAt(scheduledRunAt: string | null, queueAvailableAt: string | null, claimedUntil: string | null): string | null {
  const futureDates = [scheduledRunAt, queueAvailableAt, claimedUntil]
    .filter((value): value is string => Boolean(value))
    .map((value) => ({ value, timestamp: new Date(value).getTime() }))
    .filter(({ timestamp }) => timestamp > Date.now());

  if (futureDates.length === 0) {
    return null;
  }

  return futureDates.reduce((latest, candidate) => candidate.timestamp > latest.timestamp ? candidate : latest).value;
}

function getCapacityDeferReason(
  limits: CuratorPlanLimits,
  usage: CuratorUsageSnapshot,
  nextRunAt: string | null,
  nextQueueAvailableAt: string | null,
  claimedUntil: string | null
): string | null {
  const planBlockReason = getCuratorPlanBlockReason(limits);
  if (planBlockReason) return planBlockReason;
  if (usage.curator_runs >= limits.curator_runs_per_month) return 'monthly curator run limit exhausted';
  if (usage.curator_requests >= limits.curator_requests_per_month) return 'monthly curator request limit exhausted';
  if (usage.curator_input_tokens + usage.curator_output_tokens >= limits.curator_tokens_per_month) return 'monthly curator token limit exhausted';
  if (nextRunAt && new Date(nextRunAt).getTime() > Date.now()) return 'waiting for scheduled curator interval';
  if (nextQueueAvailableAt && new Date(nextQueueAvailableAt).getTime() > Date.now()) return 'waiting for queued curation availability';
  if (claimedUntil && new Date(claimedUntil).getTime() > Date.now()) return 'curator run already claimed';
  return null;
}
