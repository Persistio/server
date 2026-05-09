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
      [request.vault.id]
    );

    const aliasResult = await query<{ count: string }>(
      `SELECT COUNT(*)::text AS count
       FROM entity_aliases
       WHERE vault_id = $1`,
      [request.vault.id]
    );

    const contradictionResult = await query<{ last_run: string | null; arbitrations_this_week: string }>(
      `SELECT
         MAX(created_at)::timestamptz::text AS last_run,
         COUNT(*) FILTER (WHERE created_at >= date_trunc('week', now()))::text AS arbitrations_this_week
       FROM contradiction_scan_log
       WHERE vault_id = $1`,
      [request.vault.id]
    );

    const usage = usageResult.rows[0];
    const counts = memoryResult.rows[0];
    const aliases = aliasResult.rows[0];
    const contradictionScan = contradictionResult.rows[0];

    return {
      vault_id: request.vault.id,
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
  });
}
