import { query } from '../db/client';
import { getCurrentUsagePeriod } from './usage';

export interface VaultStats {
  vault_id: string;
  plan: string;
  period: string;
  memories: {
    active: number;
    candidate: number;
    needs_review: number;
    contradicted: number;
    superseded: number;
    archived: number;
    limit: number | null;
  };
  entity_aliases: number;
  contradiction_scan: {
    last_run: string | null;
    arbitrations_this_week: number;
  };
  usage: {
    ingest_events: {
      consumed: number;
      limit: number | null;
    };
    memory_adds: {
      consumed: number;
      limit: number | null;
    };
    searches: {
      consumed: number;
      limit: number | null;
    };
  };
}

export async function getVaultStats(vaultId: string, accountId: string | null = null): Promise<VaultStats | null> {
  const currentPeriod = getCurrentUsagePeriod();
  const accountFilter = accountId ? 'AND v.account_id = $3::uuid' : '';
  const usageParams = accountId ? [vaultId, currentPeriod, accountId] : [vaultId, currentPeriod];

  const usageResult = await query<{
    plan_id: string;
    period: string | null;
    ingest_events: string;
    memory_adds: string;
    searches: string;
    limits: {
      memories_max?: number;
      ingest_events_per_month?: number;
      memory_adds_per_month?: number;
      searches_per_month?: number;
    };
  }>(
    `SELECT
       v.plan_id,
       CASE WHEN vu.period = $2 THEN vu.period ELSE NULL END AS period,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.ingest_events ELSE 0 END, 0)::text AS ingest_events,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.memory_adds ELSE 0 END, 0)::text AS memory_adds,
       COALESCE(CASE WHEN vu.period = $2 THEN vu.searches ELSE 0 END, 0)::text AS searches,
       p.limits
     FROM vaults AS v
     JOIN plans AS p
       ON p.id = v.plan_id
     LEFT JOIN vault_usage AS vu
       ON vu.vault_id = v.id
     WHERE v.id = $1
       ${accountFilter}
     LIMIT 1`,
    usageParams
  );

  const usage = usageResult.rows[0];
  if (!usage) return null;

  const memoryResult = await query<{
    active: string;
    candidate: string;
    needs_review: string;
    contradicted: string;
    superseded: string;
    archived: string;
  }>(
    `SELECT
       COUNT(*) FILTER (WHERE archived_at IS NULL AND status = 'active')::text AS active,
       COUNT(*) FILTER (WHERE archived_at IS NULL AND status = 'candidate')::text AS candidate,
       COUNT(*) FILTER (WHERE archived_at IS NULL AND status = 'needs_review')::text AS needs_review,
       COUNT(*) FILTER (WHERE archived_at IS NULL AND status = 'contradicted')::text AS contradicted,
       COUNT(*) FILTER (WHERE archived_at IS NULL AND status = 'superseded')::text AS superseded,
       COUNT(*) FILTER (WHERE archived_at IS NOT NULL)::text AS archived
     FROM memories
     WHERE vault_id = $1`,
    [vaultId]
  );

  const aliasResult = await query<{ count: string }>(
    `SELECT COUNT(*)::text AS count
     FROM entity_aliases
     WHERE vault_id = $1`,
    [vaultId]
  );

  const contradictionResult = await query<{ last_run: string | null; arbitrations_this_week: string }>(
    `SELECT
       MAX(created_at)::timestamptz::text AS last_run,
       COUNT(*) FILTER (WHERE created_at >= date_trunc('week', now()))::text AS arbitrations_this_week
     FROM contradiction_scan_log
     WHERE vault_id = $1`,
    [vaultId]
  );

  const counts = memoryResult.rows[0];
  const aliases = aliasResult.rows[0];
  const contradictionScan = contradictionResult.rows[0];

  return {
    vault_id: vaultId,
    plan: usage.plan_id,
    period: usage.period ?? currentPeriod,
    memories: {
      active: Number(counts.active),
      candidate: Number(counts.candidate),
      needs_review: Number(counts.needs_review),
      contradicted: Number(counts.contradicted),
      superseded: Number(counts.superseded),
      archived: Number(counts.archived),
      limit: usage.limits.memories_max ?? null
    },
    entity_aliases: Number(aliases.count),
    contradiction_scan: {
      last_run: contradictionScan.last_run,
      arbitrations_this_week: Number(contradictionScan.arbitrations_this_week)
    },
    usage: {
      ingest_events: {
        consumed: Number(usage.ingest_events),
        limit: usage.limits.ingest_events_per_month ?? null
      },
      memory_adds: {
        consumed: Number(usage.memory_adds),
        limit: usage.limits.memory_adds_per_month ?? null
      },
      searches: {
        consumed: Number(usage.searches),
        limit: usage.limits.searches_per_month ?? null
      }
    }
  };
}
