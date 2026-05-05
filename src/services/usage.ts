import { query } from '../db/client';

export type UsageField = 'ingest_events' | 'memory_adds' | 'searches';

const usageLimitKeys: Record<UsageField, string> = {
  ingest_events: 'ingest_events_per_month',
  memory_adds: 'memory_adds_per_month',
  searches: 'searches_per_month'
};

export class QuotaExceededError extends Error {
  statusCode = 429;

  constructor(message: string) {
    super(message);
  }
}

export function getCurrentUsagePeriod(now = new Date()): string {
  return now.toISOString().slice(0, 7);
}

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

export async function checkQuota(vaultId: string, field: UsageField) {
  const period = getCurrentUsagePeriod();
  const limitKey = usageLimitKeys[field];
  const result = await query<{ consumed: string; quota_limit: string | null }>(
    `SELECT
       COALESCE(CASE WHEN vu.period = $2 THEN vu.${field} ELSE 0 END, 0)::text AS consumed,
       p.limits->>$3 AS quota_limit
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

  if (limit !== undefined && consumed >= limit) {
    throw new QuotaExceededError(`${field} quota exceeded`);
  }
}

export async function canCreateMemory(vaultId: string): Promise<boolean> {
  const result = await query<{ active_memories: string; memories_max: string | null }>(
    `SELECT
       COUNT(m.id) FILTER (WHERE m.archived_at IS NULL)::text AS active_memories,
       p.limits->>'memories_max' AS memories_max
     FROM vaults AS v
     JOIN plans AS p
       ON p.id = v.plan_id
     LEFT JOIN memories AS m
       ON m.vault_id = v.id
     WHERE v.id = $1
     GROUP BY p.limits`,
    [vaultId]
  );

  if (!result.rowCount) {
    throw new Error(`Vault ${vaultId} not found`);
  }

  const activeMemories = Number(result.rows[0].active_memories);
  const limit = result.rows[0].memories_max ? Number(result.rows[0].memories_max) : undefined;
  return limit === undefined || activeMemories < limit;
}
