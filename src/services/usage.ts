import type { FastifyReply } from 'fastify';

import { query, withTransaction } from '../db/client';

export type UsageField = 'ingest_events' | 'memory_adds' | 'searches';

type PlanId = 'free' | 'starter' | 'pro';
type LimitConfig = Partial<Record<UsageFieldLimitKey | GeminiLimitKey | 'memories_max', number>>;
type GeminiLimitKey = 'gemini_rpm' | 'gemini_tpm';
type UsageFieldLimitKey = 'ingest_events_per_month' | 'memory_adds_per_month' | 'searches_per_month';

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

interface VaultQuotaStateRow {
  current_period: string | null;
  current_value: string | null;
  quota_limit: string | null;
}

interface MemoryCapacityRow {
  active_memories: string;
  memories_max: string | null;
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

const geminiPlanDefaults: Record<PlanId, Required<Pick<LimitConfig, GeminiLimitKey>>> = {
  free: { gemini_rpm: 10, gemini_tpm: 50_000 },
  starter: { gemini_rpm: 50, gemini_tpm: 250_000 },
  pro: { gemini_rpm: 100, gemini_tpm: 500_000 }
};

// TODO: These buckets are in-process only. They reset on restart and do not coordinate across
// replicas, so precise enforcement at scale requires Redis or shared database-backed state.
const geminiRequestBuckets = new Map<string, BucketState>();
// TODO: These buckets are in-process only. They reset on restart and do not coordinate across
// replicas, so precise enforcement at scale requires Redis or shared database-backed state.
const geminiTokenBuckets = new Map<string, BucketState>();

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

// NOTE: incrementUsage does not acquire a vault-level lock and is not protected against
// concurrent races with consumeApiQuota. If both run simultaneously for the same vault,
// the FOR UPDATE lock in consumeApiQuota does not cover this path. This is a known gap
// and should be addressed if incrementUsage is ever called on a hot write path.
export async function incrementUsage(vaultId: string, field: UsageField) {
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

  await query(
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
}

export async function consumeApiQuota(vaultId: string, field: UsageField): Promise<RateLimitSnapshot> {
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
  const { limit, consumed } = await withTransaction(async (client) => {
    // Lock the vault row to serialise concurrent quota consumption for the same vault.
    // Without this, two simultaneous requests could both read "under quota" and both increment,
    // allowing the limit to be exceeded.
    const stateResult = await client.query<VaultQuotaStateRow>(
      `SELECT
         vu.period AS current_period,
         vu.${fieldColumn[field]}::text AS current_value,
         COALESCE((v.rate_limit_override->>$2), (p.limits->>$2)) AS quota_limit
       FROM vaults AS v
       JOIN plans AS p
         ON p.id = v.plan_id
       LEFT JOIN vault_usage AS vu
         ON vu.vault_id = v.id
       WHERE v.id = $1
       LIMIT 1
       FOR UPDATE OF v`,
      [vaultId, limitKey]
    );

    if (!stateResult.rowCount) {
      throw new Error(`Vault ${vaultId} not found`);
    }

    const state = stateResult.rows[0];
    const limit = state.quota_limit ? Number(state.quota_limit) : undefined;
    const consumedBefore = state.current_period === period ? Number(state.current_value ?? '0') : 0;

    if (limit !== undefined && consumedBefore >= limit) {
      throw new QuotaExceededError(`${field} quota exceeded`, {
        limit,
        remaining: 0,
        resetAtEpochSeconds,
        retryAfterSeconds: Math.max(1, resetAtEpochSeconds - Math.floor(Date.now() / 1000))
      });
    }

    const result = await client.query<AtomicQuotaConsumeRow>(
      `INSERT INTO vault_usage (vault_id, period, ingest_events, memory_adds, searches, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (vault_id) DO UPDATE
       SET period = EXCLUDED.period,
           ${assignments},
           updated_at = now()
       RETURNING ${fieldColumn[field]}::text AS consumed`,
      [
        vaultId,
        period,
        field === 'ingest_events' ? 1 : 0,
        field === 'memory_adds' ? 1 : 0,
        field === 'searches' ? 1 : 0
      ]
    );
    if (!result.rowCount) {
      throw new Error(`Vault ${vaultId} not found or upsert returned no rows`);
    }

    return {
      limit,
      consumed: Number(result.rows[0].consumed)
    };
  });

  return {
    limit: limit ?? null,
    remaining: limit === undefined ? null : Math.max(0, limit - consumed),
    resetAtEpochSeconds,
    retryAfterSeconds: null
  };
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

export async function enforceMemoryCreationLimit(vaultId: string): Promise<void> {
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

  await consumeApiQuota(vaultId, 'memory_adds');
}

export async function canCreateMemory(vaultId: string): Promise<boolean> {
  const capacity = await getMemoryCapacity(vaultId);
  return capacity.limit === undefined || capacity.activeMemories < capacity.limit;
}

export async function consumeGeminiQuota(vaultId: string, estimatedTokens: number): Promise<void> {
  const limits = await getVaultLimits(vaultId);
  const requestLimit = limits.gemini_rpm;
  const tokenLimit = limits.gemini_tpm;

  if (requestLimit !== undefined) {
    assertAndConsumeTokenBucket(
      geminiRequestBuckets,
      `${vaultId}:requests`,
      requestLimit,
      1,
      `gemini_rpm quota exceeded for vault ${vaultId}`
    );
  }

  if (tokenLimit !== undefined) {
    assertAndConsumeTokenBucket(
      geminiTokenBuckets,
      `${vaultId}:tokens`,
      tokenLimit,
      Math.max(1, estimatedTokens),
      `gemini_tpm quota exceeded for vault ${vaultId}`
    );
  }
}

export async function settleGeminiUsage(vaultId: string, estimatedTokens: number, actualTokens: number) {
  if (actualTokens <= estimatedTokens) {
    return;
  }

  const limits = await getVaultLimits(vaultId);
  if (limits.gemini_tpm === undefined) {
    return;
  }

  assertAndConsumeTokenBucket(
    geminiTokenBuckets,
    `${vaultId}:tokens`,
    limits.gemini_tpm,
    actualTokens - estimatedTokens,
    `gemini_tpm quota exceeded for vault ${vaultId}`
  );
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
  const planDefaults = geminiPlanDefaults[(row.plan_id as PlanId)] ?? geminiPlanDefaults.free;
  return {
    ...planDefaults,
    ...(row.limits ?? {}),
    ...(row.rate_limit_override ?? {})
  };
}

function assertAndConsumeTokenBucket(
  buckets: Map<string, BucketState>,
  key: string,
  limit: number,
  cost: number,
  message: string
) {
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

  if (state.tokens < cost) {
    const deficit = cost - state.tokens;
    const retryAfterSeconds = Math.max(1, Math.ceil(deficit / state.refillPerMs / 1000));
    buckets.set(key, state);
    throw new QuotaExceededError(message, {
      limit,
      remaining: Math.max(0, Math.floor(state.tokens)),
      resetAtEpochSeconds: Math.floor((now + deficit / state.refillPerMs) / 1000),
      retryAfterSeconds
    });
  }

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
  assertAndConsumeTokenBucket,
  clearGeminiBuckets() {
    geminiRequestBuckets.clear();
    geminiTokenBuckets.clear();
  },
  getGeminiBucketTokens(kind: 'requests' | 'tokens', vaultId: string) {
    const bucket = (kind === 'requests' ? geminiRequestBuckets : geminiTokenBuckets).get(`${vaultId}:${kind}`);
    return bucket?.tokens ?? null;
  },
  refillBucket
};
