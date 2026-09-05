import type { FastifyReply } from 'fastify';
import type { PoolClient } from 'pg';

import { query, withTransaction } from '../db/client';
import type {
  PlatformActor,
  VaultUsagePeriodClosedPayload,
  VaultUsagePeriodLimitField
} from '../events/platform-event';
import { usagePeriodClosedEventType } from '../events/platform-event';
import { recordCustomerMetric, type CustomerMetricSource } from './customer-metrics';

export type UsageField = 'ingest_events' | 'memory_adds' | 'searches';
export type ModelUsageRole = 'embedding' | 'extraction' | 'escalation' | 'curation';

type PlanId = 'unlimited' | 'free';
export type AiBudgetRole = 'extraction' | 'escalation' | 'curation';
type AiLimitKey =
  | 'ai_requests_per_minute'
  | 'ai_tokens_per_minute'
  | 'ai_extraction_weight'
  | 'ai_escalation_weight'
  | 'ai_curation_weight';
type LimitConfig = Partial<Record<UsageFieldLimitKey | AiLimitKey | UsagePeriodEventLimitField | 'memories_max', number>>;
type UsageFieldLimitKey = 'ingest_events_per_month' | 'memory_adds_per_month' | 'searches_per_month';
type UsagePeriodEventLimitField = VaultUsagePeriodLimitField;

interface VaultLimitRow {
  limits: LimitConfig | null;
  plan_id: string;
  rate_limit_override: LimitConfig | null;
}

interface VaultUsageRow {
  consumed: string;
  quota_limit: string | null;
}

interface AtomicQuotaConsumeRow {
  consumed: string;
}

interface ApiQuotaLimitRow {
  account_id: string | null;
  quota_limit: string | null;
}

interface MemoryCapacityRow {
  active_memories: string;
  memories_max: string | null;
}

interface ClosingUsagePeriodRow {
  account_id: string | null;
  curator_candidates_deferred: string;
  curator_candidates_processed: string;
  curator_input_tokens: string;
  curator_output_tokens: string;
  curator_requests: string;
  curator_runs: string;
  ingest_events: string;
  limits: Record<string, unknown> | null;
  memory_adds: string;
  period: string;
  plan_id: string;
  rate_limit_override: Record<string, unknown> | null;
  searches: string;
}

interface StaleUsagePeriodRow {
  vault_id: string;
}

interface BucketState {
  capacity: number;
  lastRefillMs: number;
  refillPerMs: number;
  tokens: number;
}

export interface RateLimitSnapshot {
  limit: number | null;
  remaining: number | null;
  resetAtEpochSeconds: number | null;
  retryAfterSeconds: number | null;
}

export interface ApiQuotaReservation {
  accountId: string | null;
  field: UsageField;
  period: string;
  snapshot: RateLimitSnapshot;
  source: CustomerMetricSource;
  vaultId: string;
}

export interface ModelUsageInput {
  vaultId: string;
  provider: string;
  modelRole: ModelUsageRole;
  model: string;
  requestCount?: number;
  embeddingCalls?: number;
  embeddingInputTokens?: number;
  embeddingInputChars?: number;
  promptTokens?: number;
  completionTokens?: number;
  totalTokens?: number;
  source: CustomerMetricSource;
}

export interface UsagePeriodSweepResult {
  closed: number;
  currentPeriod: string;
  selected: number;
}

interface ModelUsageScopeRow {
  account_id: string | null;
}

const usageLimitKeys: Record<UsageField, UsageFieldLimitKey> = {
  ingest_events: 'ingest_events_per_month',
  memory_adds: 'memory_adds_per_month',
  searches: 'searches_per_month'
};
const fieldColumn: Record<UsageField, string> = {
  ingest_events: 'ingest_events',
  memory_adds: 'memory_adds',
  searches: 'searches'
};
const allowedUsageFields: UsageField[] = ['ingest_events', 'memory_adds', 'searches'];
const usagePeriodEventLimitFields: UsagePeriodEventLimitField[] = [
  'ingest_events_per_month',
  'memory_adds_per_month',
  'searches_per_month',
  'curator_runs_per_month',
  'curator_requests_per_month',
  'curator_tokens_per_month'
];

const conservativeAiDefaults: Required<Pick<LimitConfig, AiLimitKey>> = {
  ai_requests_per_minute: 10,
  ai_tokens_per_minute: 50_000,
  ai_extraction_weight: 1,
  ai_escalation_weight: 2,
  ai_curation_weight: 4
};

const aiPlanDefaults: Record<PlanId, Required<Pick<LimitConfig, AiLimitKey>>> = {
  unlimited: {
    ai_requests_per_minute: 300,
    ai_tokens_per_minute: 3_000_000,
    ai_extraction_weight: 1,
    ai_escalation_weight: 2,
    ai_curation_weight: 4
  },
  free: conservativeAiDefaults
};

// TODO: These buckets are in-process only. They reset on restart and do not coordinate across
// replicas, so precise enforcement at scale requires Redis or shared database-backed state.
const aiRequestBuckets = new Map<string, BucketState>();
// TODO: These buckets are in-process only. They reset on restart and do not coordinate across
// replicas, so precise enforcement at scale requires Redis or shared database-backed state.
const aiTokenBuckets = new Map<string, BucketState>();
// TODO: Normal-ingest RPM buckets are in-process only. Use Redis/shared state if strict
// cross-replica enforcement becomes necessary.
const ingestRequestBuckets = new Map<string, BucketState>();

export class AiBudgetDeferredError extends Error {
  readonly availableAt: Date;
  readonly role: AiBudgetRole;
  readonly waitMs: number;

  constructor(role: AiBudgetRole, availableAt: Date, waitMs: number) {
    super(`AI budget deferred for ${role} until ${availableAt.toISOString()}`);
    this.name = 'AiBudgetDeferredError';
    this.availableAt = availableAt;
    this.role = role;
    this.waitMs = waitMs;
  }
}

export class QuotaExceededError extends Error {
  readonly headers: RateLimitSnapshot;
  readonly statusCode = 429;

  constructor(message: string, headers: RateLimitSnapshot) {
    super(message);
    this.name = 'QuotaExceededError';
    this.headers = headers;
  }
}

export function getCurrentUsagePeriod(now = new Date()): string {
  return now.toISOString().slice(0, 7);
}

export function applyRateLimitHeaders(reply: FastifyReply, snapshot: RateLimitSnapshot) {
  if (snapshot.limit !== null) {
    reply.header('X-RateLimit-Limit', snapshot.limit);
  }
  if (snapshot.remaining !== null) {
    reply.header('X-RateLimit-Remaining', snapshot.remaining);
  }
  if (snapshot.resetAtEpochSeconds !== null) {
    reply.header('X-RateLimit-Reset', snapshot.resetAtEpochSeconds);
  }
  if (snapshot.retryAfterSeconds !== null) {
    reply.header('Retry-After', snapshot.retryAfterSeconds);
  }
}

export function isPremiumPlan(planId: string): boolean {
  return planId === 'unlimited';
}

export function consumeNormalIngestRateLimit(vaultId: string, planId: string, requestsPerMinute: number): RateLimitSnapshot {
  if (isPremiumPlan(planId)) {
    return {
      limit: null,
      remaining: null,
      resetAtEpochSeconds: null,
      retryAfterSeconds: null
    };
  }

  const limit = Math.max(1, requestsPerMinute);
  const key = `${vaultId}:normal-ingest`;
  const waitMs = getTokenBucketWaitMs(ingestRequestBuckets, key, limit, 1);
  if (waitMs > 0) {
    throw new QuotaExceededError('ingest rate limit exceeded', {
      limit,
      remaining: 0,
      resetAtEpochSeconds: Math.floor((Date.now() + waitMs) / 1000),
      retryAfterSeconds: Math.max(1, Math.ceil(waitMs / 1000))
    });
  }

  consumeTokenBucket(ingestRequestBuckets, key, limit, 1);
  const remaining = Math.max(0, Math.floor(ingestRequestBuckets.get(key)?.tokens ?? 0));
  return {
    limit,
    remaining,
    resetAtEpochSeconds: Math.floor((Date.now() + 60_000) / 1000),
    retryAfterSeconds: null
  };
}

// NOTE: incrementUsage is intended for low-volume internal adjustments. Hot API paths
// use consumeApiQuota so quota checks and increments stay atomic.
export async function incrementUsage(
  vaultId: string,
  field: UsageField,
  source: CustomerMetricSource = 'system'
) {
  const period = getCurrentUsagePeriod();
  const columns: UsageField[] = ['ingest_events', 'memory_adds', 'searches'];
  const assignments = columns.map((column) => {
    const resetValue = column === field ? '1' : '0';
    const incrementValue = column === field ? `vault_usage.${column} + 1` : `vault_usage.${column}`;
    return `${column} = CASE
      WHEN vault_usage.period = EXCLUDED.period THEN ${incrementValue}
      ELSE ${resetValue}
    END`;
  }).join(',\n           ');

  const accountId = await withTransaction(async (client) => {
    const vaultResult = await client.query<{ account_id: string | null }>(
      `SELECT account_id::text AS account_id
       FROM vaults
       WHERE id = $1
       LIMIT 1
       FOR KEY SHARE`,
      [vaultId]
    );
    await writeUsagePeriodClosedEventIfNeeded(client, vaultId, period);
    await client.query(
      `INSERT INTO vault_usage (vault_id, period, ${columns.join(', ')}, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (vault_id) DO UPDATE
       SET period = EXCLUDED.period,
           ${assignments},
           updated_at = now()`,
      [
        vaultId,
        period,
        field === 'ingest_events' ? 1 : 0,
        field === 'memory_adds' ? 1 : 0,
        field === 'searches' ? 1 : 0
      ]
    );
    return vaultResult.rows[0]?.account_id ?? null;
  });
  recordUsageQuotaDelta({
    accountId,
    delta: 1,
    field,
    source,
    vaultId
  });
}

export async function consumeApiQuota(
  vaultId: string,
  field: UsageField,
  source: CustomerMetricSource = 'api'
): Promise<RateLimitSnapshot> {
  const reservation = await reserveApiQuota(vaultId, field, source);
  return reservation.snapshot;
}

export async function reserveApiQuota(
  vaultId: string,
  field: UsageField,
  source: CustomerMetricSource = 'api'
): Promise<ApiQuotaReservation> {
  const result = await withTransaction((client) => consumeApiQuotaWithPeriodInTransaction(client, vaultId, field));
  recordUsageQuotaDelta({
    accountId: result.accountId,
    delta: 1,
    field,
    source,
    vaultId
  });
  return {
    accountId: result.accountId,
    field,
    period: result.period,
    snapshot: result.snapshot,
    source,
    vaultId
  };
}

export async function consumeApiQuotaInTransaction(client: PoolClient, vaultId: string, field: UsageField): Promise<RateLimitSnapshot> {
  const result = await consumeApiQuotaWithPeriodInTransaction(client, vaultId, field);
  return result.snapshot;
}

async function consumeApiQuotaWithPeriodInTransaction(
  client: PoolClient,
  vaultId: string,
  field: UsageField
): Promise<{ accountId: string | null; period: string; snapshot: RateLimitSnapshot }> {
  if (!allowedUsageFields.includes(field)) {
    throw new Error(`Invalid usage field: ${field}`);
  }

  const period = getCurrentUsagePeriod();
  const limitKey = usageLimitKeys[field];
  const columns: UsageField[] = ['ingest_events', 'memory_adds', 'searches'];
  const assignments = columns.map((column) => {
    const resetValue = column === field ? '1' : '0';
    const incrementValue = column === field ? `vault_usage.${column} + 1` : `vault_usage.${column}`;
    return `${column} = CASE
      WHEN vault_usage.period = EXCLUDED.period THEN ${incrementValue}
      ELSE ${resetValue}
    END`;
  }).join(',\n           ');
  const resetAtEpochSeconds = getNextUsageResetEpochSeconds();
  const limitResult = await client.query<ApiQuotaLimitRow>(
    `SELECT v.account_id::text AS account_id,
            COALESCE((v.rate_limit_override->>$2), (p.limits->>$2)) AS quota_limit
     FROM vaults AS v
     JOIN plans AS p
       ON p.id = v.plan_id
     WHERE v.id = $1
     LIMIT 1
     FOR KEY SHARE OF v`,
    [vaultId, limitKey]
  );

  if (!limitResult.rowCount) {
    throw new Error(`Vault ${vaultId} not found`);
  }

  const limit = limitResult.rows[0].quota_limit ? Number(limitResult.rows[0].quota_limit) : undefined;
  if (limit !== undefined && limit <= 0) {
    throw new QuotaExceededError(`${field} quota exceeded`, {
      limit,
      remaining: 0,
      resetAtEpochSeconds,
      retryAfterSeconds: Math.max(1, resetAtEpochSeconds - Math.floor(Date.now() / 1000))
    });
  }

  await writeUsagePeriodClosedEventIfNeeded(client, vaultId, period);

  const result = await client.query<AtomicQuotaConsumeRow>(
    `INSERT INTO vault_usage (vault_id, period, ingest_events, memory_adds, searches, updated_at)
     VALUES ($1, $2, $3, $4, $5, now())
     ON CONFLICT (vault_id) DO UPDATE
     SET period = EXCLUDED.period,
         ${assignments},
         updated_at = now()
     WHERE $6::int IS NULL
        OR vault_usage.period <> EXCLUDED.period
        OR vault_usage.${fieldColumn[field]} < $6::int
     RETURNING ${fieldColumn[field]}::text AS consumed`,
    [
      vaultId,
      period,
      field === 'ingest_events' ? 1 : 0,
      field === 'memory_adds' ? 1 : 0,
      field === 'searches' ? 1 : 0,
      limit ?? null
    ]
  );

  if (!result.rowCount) {
    throw new QuotaExceededError(`${field} quota exceeded`, {
      limit: limit ?? null,
      remaining: 0,
      resetAtEpochSeconds,
      retryAfterSeconds: Math.max(1, resetAtEpochSeconds - Math.floor(Date.now() / 1000))
    });
  }

  const consumed = Number(result.rows[0].consumed);

  return {
    accountId: limitResult.rows[0].account_id,
    period,
    snapshot: {
      limit: limit ?? null,
      remaining: limit === undefined ? null : Math.max(0, limit - consumed),
      resetAtEpochSeconds,
      retryAfterSeconds: null
    }
  };
}

export async function writeUsagePeriodClosedEventIfNeeded(
  client: PoolClient,
  vaultId: string,
  currentPeriod = getCurrentUsagePeriod()
): Promise<boolean> {
  const result = await client.query<ClosingUsagePeriodRow>(
    `SELECT
       vu.period,
       vu.ingest_events::text AS ingest_events,
       vu.memory_adds::text AS memory_adds,
       vu.searches::text AS searches,
       vu.curator_runs::text AS curator_runs,
       vu.curator_requests::text AS curator_requests,
       vu.curator_input_tokens::text AS curator_input_tokens,
       vu.curator_output_tokens::text AS curator_output_tokens,
       vu.curator_candidates_processed::text AS curator_candidates_processed,
       vu.curator_candidates_deferred::text AS curator_candidates_deferred,
       v.account_id::text AS account_id,
       v.plan_id,
       p.limits,
       v.rate_limit_override
     FROM vault_usage AS vu
     JOIN vaults AS v
       ON v.id = vu.vault_id
     JOIN plans AS p
       ON p.id = v.plan_id
     WHERE vu.vault_id = $1
     LIMIT 1
     FOR UPDATE OF vu`,
    [vaultId]
  );

  if (!result.rowCount || result.rows[0].period === currentPeriod) {
    return false;
  }

  const row = result.rows[0];
  const actor: PlatformActor = {
    id: null,
    type: 'system'
  };
  const payload: VaultUsagePeriodClosedPayload = {
    platform_vault_id: vaultId,
    account_id: row.account_id,
    workspace_id: row.account_id ?? '',
    vault_id: vaultId,
    period: row.period,
    plan_id: row.plan_id,
    actor,
    counts: {
      ingest_events: Number(row.ingest_events),
      memory_adds: Number(row.memory_adds),
      searches: Number(row.searches),
      curator_runs: Number(row.curator_runs)
    },
    sensitivity: 'metadata_only',
    summary: `Usage period ${row.period} closed`,
    usage: {
      ingest_events: Number(row.ingest_events),
      memory_adds: Number(row.memory_adds),
      searches: Number(row.searches),
      curator_runs: Number(row.curator_runs),
      curator_requests: Number(row.curator_requests),
      curator_input_tokens: Number(row.curator_input_tokens),
      curator_output_tokens: Number(row.curator_output_tokens),
      curator_candidates_processed: Number(row.curator_candidates_processed),
      curator_candidates_deferred: Number(row.curator_candidates_deferred)
    },
    limits: buildUsagePeriodEventLimits(row.limits, row.rate_limit_override)
  };

  await client.query(
    `INSERT INTO platform_event_outbox (
       event_id, event_type, schema_version, occurred_at, subject, payload
     )
     VALUES (gen_random_uuid(), $2, 1, now(), $1, $3::jsonb)
     ON CONFLICT DO NOTHING`,
    [`vault:${vaultId}`, usagePeriodClosedEventType, JSON.stringify(payload)]
  );

  return true;
}

export async function closeStaleUsagePeriods(
  batchSize = 100,
  currentPeriod = getCurrentUsagePeriod()
): Promise<UsagePeriodSweepResult> {
  return await withTransaction(async (client) => {
    const result = await client.query<StaleUsagePeriodRow>(
      `SELECT vu.vault_id::text AS vault_id
       FROM vault_usage AS vu
       WHERE vu.period < $1
       ORDER BY vu.period ASC, vu.updated_at ASC, vu.vault_id ASC
       LIMIT $2
       FOR UPDATE SKIP LOCKED`,
      [currentPeriod, batchSize]
    );

    let closed = 0;
    for (const row of result.rows) {
      if (await writeUsagePeriodClosedEventIfNeeded(client, row.vault_id, currentPeriod)) {
        closed += 1;
      }
    }

    if (result.rows.length) {
      await client.query(
        `UPDATE vault_usage
         SET period = $1,
             ingest_events = 0,
             memory_adds = 0,
             searches = 0,
             curator_runs = 0,
             curator_requests = 0,
             curator_input_tokens = 0,
             curator_output_tokens = 0,
             curator_candidates_processed = 0,
             curator_candidates_deferred = 0,
             updated_at = now()
         WHERE vault_id = ANY($2::uuid[])
           AND period < $1`,
        [currentPeriod, result.rows.map((row) => row.vault_id)]
      );
    }

    return {
      closed,
      currentPeriod,
      selected: result.rows.length
    };
  });
}

export async function refundApiQuotaReservation(reservation: ApiQuotaReservation): Promise<void> {
  if (!allowedUsageFields.includes(reservation.field)) {
    throw new Error(`Invalid usage field: ${reservation.field}`);
  }

  const column = fieldColumn[reservation.field];
  const result = await query(
    `UPDATE vault_usage
     SET ${column} = GREATEST(${column} - 1, 0),
         updated_at = now()
     WHERE vault_id = $1
       AND period = $2
       AND ${column} > 0
     RETURNING vault_id`,
    [reservation.vaultId, reservation.period]
  );
  if (result.rowCount) {
    recordUsageQuotaDelta({
      accountId: reservation.accountId,
      delta: -1,
      field: reservation.field,
      source: reservation.source,
      vaultId: reservation.vaultId
    });
  }
}

export async function checkQuota(vaultId: string, field: UsageField): Promise<RateLimitSnapshot> {
  const period = getCurrentUsagePeriod();
  const limitKey = usageLimitKeys[field];
  const result = await query<VaultUsageRow>(
    `SELECT
       COALESCE(CASE WHEN vu.period = $2 THEN vu.${fieldColumn[field]} ELSE 0 END, 0)::text AS consumed,
       COALESCE((v.rate_limit_override->>$3), (p.limits->>$3)) AS quota_limit
     FROM vaults AS v
     JOIN plans AS p
       ON p.id = v.plan_id
     LEFT JOIN vault_usage AS vu
       ON vu.vault_id = v.id
     WHERE v.id = $1
     LIMIT 1`,
    [vaultId, period, limitKey]
  );

  if (!result.rowCount) {
    throw new Error(`Vault ${vaultId} not found`);
  }

  const consumed = Number(result.rows[0].consumed);
  const limit = result.rows[0].quota_limit ? Number(result.rows[0].quota_limit) : undefined;
  const resetAtEpochSeconds = getNextUsageResetEpochSeconds();

  if (limit !== undefined && consumed >= limit) {
    throw new QuotaExceededError(`${field} quota exceeded`, {
      limit,
      remaining: 0,
      resetAtEpochSeconds,
      retryAfterSeconds: Math.max(1, resetAtEpochSeconds - Math.floor(Date.now() / 1000))
    });
  }

  return {
    limit: limit ?? null,
    remaining: limit === undefined ? null : Math.max(0, limit - consumed),
    resetAtEpochSeconds,
    retryAfterSeconds: null
  };
}

export async function enforceMemoryCreationLimit(vaultId: string, source: CustomerMetricSource = 'api'): Promise<void> {
  const capacity = await getMemoryCapacity(vaultId);

  if (capacity.limit !== undefined && capacity.activeMemories >= capacity.limit) {
    const resetAtEpochSeconds = getNextUsageResetEpochSeconds();
    throw new QuotaExceededError('memories_max quota exceeded', {
      limit: capacity.limit,
      remaining: 0,
      resetAtEpochSeconds,
      retryAfterSeconds: Math.max(1, resetAtEpochSeconds - Math.floor(Date.now() / 1000))
    });
  }

  await consumeApiQuota(vaultId, 'memory_adds', source);
}

export function recordMemoryCountDelta(
  vaultId: string,
  accountId: string | null,
  delta: number,
  source: CustomerMetricSource = 'api'
): void {
  if (delta === 0) return;
  recordQuotaDelta({
    accountId,
    delta,
    operation: 'memory_count',
    source,
    vaultId
  });
}

export async function canCreateMemory(vaultId: string): Promise<boolean> {
  const capacity = await getMemoryCapacity(vaultId);
  return capacity.limit === undefined || capacity.activeMemories < capacity.limit;
}

export async function acquireAiBudget(vaultId: string, role: AiBudgetRole, estimatedTokens: number): Promise<void> {
  const limits = await getVaultLimits(vaultId);
  const requestLimit = limits.ai_requests_per_minute;
  const tokenLimit = limits.ai_tokens_per_minute;
  const weight = getAiRoleWeight(limits, role);
  const waitMs = Math.max(
    requestLimit !== undefined
      ? getTokenBucketWaitMs(aiRequestBuckets, `${vaultId}:${role}:requests`, requestLimit, weight)
      : 0,
    tokenLimit !== undefined
      ? getTokenBucketWaitMs(aiTokenBuckets, `${vaultId}:${role}:tokens`, tokenLimit, Math.max(1, estimatedTokens) * weight)
      : 0
  );

  if (waitMs > 0) {
    throw new AiBudgetDeferredError(role, new Date(Date.now() + waitMs), waitMs);
  }

  if (requestLimit !== undefined) consumeTokenBucket(aiRequestBuckets, `${vaultId}:${role}:requests`, requestLimit, weight);
  if (tokenLimit !== undefined) consumeTokenBucket(aiTokenBuckets, `${vaultId}:${role}:tokens`, tokenLimit, Math.max(1, estimatedTokens) * weight);
}

export async function settleAiUsage(vaultId: string, role: AiBudgetRole, estimatedTokens: number, actualTokens: number) {
  if (actualTokens <= estimatedTokens) {
    return;
  }

  const limits = await getVaultLimits(vaultId);
  if (limits.ai_tokens_per_minute === undefined) {
    return;
  }

  const weight = getAiRoleWeight(limits, role);
  // Settlement may push the bucket negative when actual usage exceeds the estimate.
  // That is intentional debt: later work waits for refill rather than erasing overage.
  consumeTokenBucket(aiTokenBuckets, `${vaultId}:${role}:tokens`, limits.ai_tokens_per_minute, (actualTokens - estimatedTokens) * weight);
}

export async function recordModelUsage(input: ModelUsageInput, client?: Pick<PoolClient, 'query'>): Promise<void> {
  const period = getCurrentUsagePeriod();
  const requestCount = Math.max(0, Math.trunc(input.requestCount ?? 0));
  const embeddingCalls = Math.max(0, Math.trunc(input.embeddingCalls ?? 0));
  const embeddingInputTokens = Math.max(0, Math.trunc(input.embeddingInputTokens ?? 0));
  const embeddingInputChars = Math.max(0, Math.trunc(input.embeddingInputChars ?? 0));
  const promptTokens = Math.max(0, Math.trunc(input.promptTokens ?? 0));
  const completionTokens = Math.max(0, Math.trunc(input.completionTokens ?? 0));
  const totalTokens = Math.max(0, Math.trunc(input.totalTokens ?? promptTokens + completionTokens));

  const execute = client ? client.query.bind(client) : query;
  const result = await execute(
    `WITH upsert AS (
       INSERT INTO vault_model_usage (
         vault_id, period, provider, model_role, model, request_count,
         embedding_calls, embedding_input_tokens, embedding_input_chars,
         prompt_tokens, completion_tokens, total_tokens, updated_at
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
       ON CONFLICT (vault_id, period, provider, model_role, model)
       DO UPDATE SET
         request_count = vault_model_usage.request_count + EXCLUDED.request_count,
         embedding_calls = vault_model_usage.embedding_calls + EXCLUDED.embedding_calls,
         embedding_input_tokens = vault_model_usage.embedding_input_tokens + EXCLUDED.embedding_input_tokens,
         embedding_input_chars = vault_model_usage.embedding_input_chars + EXCLUDED.embedding_input_chars,
         prompt_tokens = vault_model_usage.prompt_tokens + EXCLUDED.prompt_tokens,
         completion_tokens = vault_model_usage.completion_tokens + EXCLUDED.completion_tokens,
         total_tokens = vault_model_usage.total_tokens + EXCLUDED.total_tokens,
         updated_at = now()
       RETURNING vault_id
     )
     SELECT v.account_id::text AS account_id
     FROM upsert
     JOIN vaults AS v
       ON v.id = upsert.vault_id`,
    [
      input.vaultId,
      period,
      input.provider,
      input.modelRole,
      input.model,
      requestCount,
      embeddingCalls,
      embeddingInputTokens,
      embeddingInputChars,
      promptTokens,
      completionTokens,
      totalTokens
    ]
  ) as { rows: ModelUsageScopeRow[] };

  const accountId = result.rows[0]?.account_id ?? null;
  if (!accountId) return;

  recordCustomerMetric({
    event_type: 'model_usage',
    model: input.model,
    model_request_count: requestCount,
    model_role: input.modelRole,
    provider: input.provider,
    source: input.source,
    vault_id: input.vaultId,
    workspace_id: accountId,
    ...(completionTokens ? { completion_tokens: completionTokens } : {}),
    ...(embeddingInputChars ? { embedding_input_chars: embeddingInputChars } : {}),
    ...(embeddingInputTokens ? { embedding_input_tokens: embeddingInputTokens } : {}),
    ...(promptTokens ? { prompt_tokens: promptTokens } : {}),
    ...(totalTokens ? { total_tokens: totalTokens } : {})
  });
}

function recordUsageQuotaDelta(input: {
  accountId: string | null;
  delta: number;
  field: UsageField;
  source: CustomerMetricSource;
  vaultId: string;
}): void {
  recordQuotaDelta({
    accountId: input.accountId,
    delta: input.delta,
    operation: input.field,
    source: input.source,
    vaultId: input.vaultId
  });
}

function recordQuotaDelta(input: {
  accountId: string | null;
  delta: number;
  operation: UsageField | 'memory_count';
  source: CustomerMetricSource;
  vaultId: string;
}): void {
  if (!input.accountId || input.delta === 0) return;

  const base = {
    event_type: 'quota_delta' as const,
    source: input.source,
    vault_id: input.vaultId,
    workspace_id: input.accountId
  };

  switch (input.operation) {
    case 'ingest_events':
      recordCustomerMetric({
        ...base,
        ingest_events_delta: input.delta,
        operation: 'ingest_events'
      });
      return;
    case 'memory_adds':
      recordCustomerMetric({
        ...base,
        memory_adds_delta: input.delta,
        operation: 'memory_adds'
      });
      return;
    case 'memory_count':
      recordCustomerMetric({
        ...base,
        memory_count_delta: input.delta,
        operation: 'memory_count'
      });
      return;
    case 'searches':
      recordCustomerMetric({
        ...base,
        operation: 'searches',
        searches_delta: input.delta
      });
      return;
  }
}

async function getMemoryCapacity(vaultId: string): Promise<{ activeMemories: number; limit: number | undefined }> {
  const result = await query<MemoryCapacityRow>(
    `SELECT
       COUNT(m.id) FILTER (WHERE m.archived_at IS NULL)::text AS active_memories,
       COALESCE((v.rate_limit_override->>'memories_max'), (p.limits->>'memories_max')) AS memories_max
     FROM vaults AS v
     JOIN plans AS p
       ON p.id = v.plan_id
     LEFT JOIN memories AS m
       ON m.vault_id = v.id
     WHERE v.id = $1
     GROUP BY p.limits, v.rate_limit_override`,
    [vaultId]
  );

  if (!result.rowCount) {
    throw new Error(`Vault ${vaultId} not found`);
  }

  return {
    activeMemories: Number(result.rows[0].active_memories),
    limit: result.rows[0].memories_max ? Number(result.rows[0].memories_max) : undefined
  };
}

async function getVaultLimits(vaultId: string): Promise<LimitConfig> {
  const result = await query<VaultLimitRow>(
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
  const planDefaults = aiPlanDefaults[(row.plan_id as PlanId)] ?? conservativeAiDefaults;
  return {
    ...planDefaults,
    ...(row.limits ?? {}),
    ...(row.rate_limit_override ?? {})
  };
}

function getAiRoleWeight(limits: LimitConfig, role: AiBudgetRole): number {
  const key: Record<AiBudgetRole, AiLimitKey> = {
    extraction: 'ai_extraction_weight',
    escalation: 'ai_escalation_weight',
    curation: 'ai_curation_weight'
  };
  return Math.max(1, limits[key[role]] ?? 1);
}

function buildUsagePeriodEventLimits(
  planLimits: Record<string, unknown> | null,
  vaultOverride: Record<string, unknown> | null
): Partial<Record<VaultUsagePeriodLimitField, number>> {
  const merged = { ...(planLimits ?? {}), ...(vaultOverride ?? {}) };
  const eventLimits: Partial<Record<VaultUsagePeriodLimitField, number>> = {};

  for (const field of usagePeriodEventLimitFields) {
    const value = readFiniteNumber(merged[field]);
    if (value !== undefined) {
      eventLimits[field] = value;
    }
  }

  return eventLimits;
}

function readFiniteNumber(value: unknown): number | undefined {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  return Number.isFinite(parsed) ? parsed : undefined;
}

function getTokenBucketWaitMs(
  buckets: Map<string, BucketState>,
  key: string,
  limit: number,
  cost: number
): number {
  const now = Date.now();
  // Persisting the refilled snapshot during the eligibility check is intentional:
  // deferral scheduling and the later consume path must observe the same bucket timeline.
  const state = refillBucket(
    buckets.get(key) ?? {
      capacity: limit,
      lastRefillMs: now,
      refillPerMs: limit / 60_000,
      tokens: limit
    },
    limit,
    now
  );

  buckets.set(key, state);
  if (state.tokens >= cost) return 0;
  const deficit = cost - state.tokens;
  return Math.max(1, Math.ceil(deficit / state.refillPerMs));
}

function consumeTokenBucket(buckets: Map<string, BucketState>, key: string, limit: number, cost: number) {
  const now = Date.now();
  const state = refillBucket(
    buckets.get(key) ?? {
      capacity: limit,
      lastRefillMs: now,
      refillPerMs: limit / 60_000,
      tokens: limit
    },
    limit,
    now
  );
  state.tokens -= cost;
  buckets.set(key, state);
}

function refillBucket(state: BucketState, capacity: number, now: number): BucketState {
  const elapsedMs = Math.max(0, now - state.lastRefillMs);
  return {
    capacity,
    lastRefillMs: now,
    refillPerMs: capacity / 60_000,
    tokens: Math.min(capacity, state.tokens + elapsedMs * (capacity / 60_000))
  };
}

function getNextUsageResetEpochSeconds(now = new Date()): number {
  const nextMonthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1, 0, 0, 0, 0));
  return Math.floor(nextMonthStart.getTime() / 1000);
}

export const usageTestInternals = {
  clearAiBuckets() {
    aiRequestBuckets.clear();
    aiTokenBuckets.clear();
    ingestRequestBuckets.clear();
  },
  clearIngestBuckets() {
    ingestRequestBuckets.clear();
  },
  getIngestBucketTokens(vaultId: string) {
    return ingestRequestBuckets.get(`${vaultId}:normal-ingest`)?.tokens ?? null;
  },
  getAiBucketTokens(kind: 'requests' | 'tokens', vaultId: string, role: AiBudgetRole) {
    const bucket = (kind === 'requests' ? aiRequestBuckets : aiTokenBuckets).get(`${vaultId}:${role}:${kind}`);
    return bucket?.tokens ?? null;
  },
  getTokenBucketWaitMs,
  refillBucket
};
