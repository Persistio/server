import { workerWakeQuery } from './worker-wake-query';
import type { Pool } from 'pg';
import { EXTRACTION_QUEUE_BLOB_READY_PREDICATE } from './extraction-queue-eligibility';
import { MIN_CURATOR_INPUT_TOKENS } from './curator-limits';
import type { WorkerWakeKind } from './worker-wake-types';

/** Observes business state; never claims work or changes its retry/lease/budget state.
 * Eligibility is ultimately rechecked by the existing business worker. */
export class WorkerWakeReconciliation {
  constructor(
    private readonly db: Pick<Pool, 'connect'>,
    private readonly options: { curationEnabled?: boolean; contradictionEnabled?: boolean } = {}
  ) {}

  async dueAt(kind: WorkerWakeKind): Promise<Date | null> {
    if (kind === 'curation' && !this.options.curationEnabled) return null;
    if (kind === 'contradiction' && !this.options.contradictionEnabled) return null;
    const result = await workerWakeQuery<{ due_at: Date | null }>(this.db, `SELECT min(due_at) AS due_at FROM (${workerDueRowsSql[kind]}) due`);
    return result.rows[0]?.due_at ?? null;
  }
}

export const workerDueRowsSql: Record<WorkerWakeKind, string> = {
  extraction: `SELECT eq.enqueued_at AS age_at, GREATEST(eq.available_at,
      CASE WHEN eq.claim_token IS NOT NULL THEN eq.lease_expires_at END) AS due_at
    FROM extraction_queue eq WHERE ${EXTRACTION_QUEUE_BLOB_READY_PREDICATE}`,
  curation: `WITH capacity AS (
    SELECT cq.enqueued_at, cq.available_at, cq.claim_token, cq.lease_expires_at,
      vcs.next_curator_run_at, vcs.curator_claimed_until,
      COALESCE(vu.period, '') = to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM') AS current_period,
      COALESCE(vu.curator_runs, 0) AS runs, COALESCE(vu.curator_requests, 0) AS requests,
      COALESCE(vu.curator_input_tokens + vu.curator_output_tokens, 0) AS tokens,
      p.limits || jsonb_strip_nulls(COALESCE(v.rate_limit_override, '{}'::jsonb)) AS limits
    FROM curation_queue cq JOIN vaults v ON v.id = cq.vault_id JOIN plans p ON p.id = v.plan_id
    LEFT JOIN vault_usage vu ON vu.vault_id = v.id
    LEFT JOIN vault_curation_state vcs ON vcs.vault_id = v.id
  ) SELECT enqueued_at AS age_at, GREATEST(available_at,
      CASE WHEN claim_token IS NOT NULL THEN lease_expires_at END,
      next_curator_run_at, curator_claimed_until,
      CASE WHEN current_period AND (
        runs >= NULLIF(limits->>'curator_runs_per_month', '')::int OR
        requests >= NULLIF(limits->>'curator_requests_per_month', '')::int OR
        tokens >= NULLIF(limits->>'curator_tokens_per_month', '')::int)
      THEN (date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month') AT TIME ZONE 'UTC' END
    ) AS due_at FROM capacity
    WHERE COALESCE((limits->>'curator_enabled')::boolean, false)
      AND COALESCE((limits->>'curator_jobs_per_run')::int, 0) > 0
      AND COALESCE((limits->>'curator_candidates_per_run')::int, 0) > 0
      AND COALESCE((limits->>'curator_candidates_per_call')::int, 0) > 0
      AND COALESCE((limits->>'curator_input_tokens_per_call')::int, 0) >= ${MIN_CURATOR_INPUT_TOKENS}
      AND COALESCE((limits->>'curator_output_tokens_per_call')::int, 0) > 0
      AND COALESCE(NULLIF(limits->>'curator_tokens_per_month', '')::int, 0) > 0
      AND COALESCE(NULLIF(limits->>'curator_requests_per_month', '')::int, 1) > 0
      AND COALESCE(NULLIF(limits->>'curator_runs_per_month', '')::int, 1) > 0`,
  contradiction: `SELECT next_visit_at AS age_at, next_visit_at AS due_at FROM memory_contradiction_pending_vaults WHERE pending_count > 0`,
  platform_outbox: `SELECT occurred_at AS age_at, CASE WHEN status = 'pending' THEN now() ELSE COALESCE(next_retry_at, now()) END
    AS due_at FROM platform_event_outbox WHERE status IN ('pending', 'failed')`,
  usage_periods: `SELECT (to_date(period || '-01','YYYY-MM-DD')::timestamp + interval '1 month') AT TIME ZONE 'UTC' AS age_at,
    CASE WHEN period < to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM') THEN now()
    ELSE (date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month') AT TIME ZONE 'UTC' END
    AS due_at FROM vault_usage WHERE period <= to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')`,
  blob_reconciliation: `SELECT updated_at AS age_at, updated_at + interval '15 minutes' AS due_at FROM raw_chunk_blob_write_intents`
};
