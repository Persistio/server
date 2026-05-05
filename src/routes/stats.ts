import type { FastifyInstance } from 'fastify';

import { query } from '../db/client';
import { requireVaultAuth } from '../middleware/auth';
import { getCurrentUsagePeriod } from '../services/usage';

export async function registerStatsRoutes(app: FastifyInstance) {
  app.get('/stats', { preHandler: requireVaultAuth }, async (request) => {
    const currentPeriod = getCurrentUsagePeriod();

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
       LIMIT 1`,
      [request.vault.id, currentPeriod]
    );

    const memoryResult = await query<{ active: string; archived: string }>(
      `SELECT
         COUNT(*) FILTER (WHERE archived_at IS NULL)::text AS active,
         COUNT(*) FILTER (WHERE archived_at IS NOT NULL)::text AS archived
       FROM memories
       WHERE vault_id = $1`,
      [request.vault.id]
    );

    const usage = usageResult.rows[0];
    const counts = memoryResult.rows[0];

    return {
      vault_id: request.vault.id,
      plan: usage.plan_id,
      period: usage.period ?? currentPeriod,
      memories: {
        active: Number(counts.active),
        archived: Number(counts.archived),
        limit: usage.limits.memories_max ?? null
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
  });
}
