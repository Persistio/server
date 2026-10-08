import type { Pool, PoolClient } from 'pg';
import { getConfig } from '../config';
import { createOperationalLogger } from '../operational-metadata';
import { workerDueRowsSql } from './worker-wake-reconciliation';
import { WORKER_WAKE_KINDS, type WorkerWakeKind } from './worker-wake-types';
import { workerWakeQuery } from './worker-wake-query';

const logger = createOperationalLogger('shadow-observability');
// Covers the largest supported observer interval (24h) plus export grace (1h).
// Immutable terminal history, not an unresolved-work gauge.
const recentDeadLetterWindowMs = 25 * 60 * 60 * 1000;
export function shadowWorkerCycle(kind: WorkerWakeKind, outcome: 'completed' | 'failed' | 'stale' | 'busy', durationMs: number): void {
  try {
    if (!getConfig().TELEMETRY_SHADOW_ENABLED) return;
    logger.info({event:'shadow_worker_cycle',shadow_kind:kind,shadow_outcome:outcome,duration_ms:durationMs});
  } catch { /* Diagnostics cannot affect task acknowledgement. */ }
}

/** Successful bounded pass, including an empty pass; not a per-job completion SLO.
 * Use inside the final owned SQL transaction for a maintenance class when available. */
export async function recordShadowCycleSuccess(client: PoolClient, kind: WorkerWakeKind): Promise<void> {
  if (!getConfig().TELEMETRY_SHADOW_ENABLED) return;
  await client.query('SAVEPOINT shadow_cycle_observation');
  try {
    await client.query(`INSERT INTO worker_observation_success(kind,succeeded_at) VALUES($1,clock_timestamp())
      ON CONFLICT(kind) DO UPDATE SET succeeded_at=GREATEST(worker_observation_success.succeeded_at,EXCLUDED.succeeded_at)`, [kind]);
  } catch {
    // This diagnostic must not turn settled business work into another task run.
    // Missing/stale last-cycle evidence remains visible to the observer.
    await client.query('ROLLBACK TO SAVEPOINT shadow_cycle_observation');
  } finally { await client.query('RELEASE SAVEPOINT shadow_cycle_observation'); }
}

export interface WorkerObservation {
  event: 'shadow_worker_observation'; shadow_kind: WorkerWakeKind | 'customer_metrics';
  shadow_health: 'ok' | 'monitor_error'; observed_at_ms: number;
  eligible_depth?: number; oldest_due_age_ms?: number; deferred_depth?: number;
  expired_leases?: number; dead_letters?: number; recent_dead_letter_window_ms?: number; last_cycle_success_age_ms?: number;
}

/** Explicitly scheduled by a host, no timer or import-time SQL. Failure never emits
 * fabricated zeros. Each kind has a separate external absence signal. */
export async function observeWorkerShadow(
  db: Pick<Pool,'connect'>,
  options: { enabled?: boolean; curationEnabled?: boolean; contradictionEnabled?: boolean;
    emit?: (observation: WorkerObservation) => void } = {}
): Promise<WorkerObservation[]> {
  if (!options.enabled) return [];
  const output: WorkerObservation[] = [];
  for (const kind of [...WORKER_WAKE_KINDS, 'customer_metrics'] as const) {
    if (kind === 'curation' && !options.curationEnabled || kind === 'contradiction' && !options.contradictionEnabled) continue;
    let observation: WorkerObservation;
    try {
      const dueSql = kind === 'customer_metrics'
        ? `SELECT event_ts AS age_at, GREATEST(available_at,lease_expires_at) AS due_at FROM customer_metric_shadow_outbox WHERE status='pending'`
        : workerDueRowsSql[kind];
      const leases = kind === 'extraction' || kind === 'curation'
        ? `(SELECT count(*) FROM ${kind}_queue WHERE claim_token IS NOT NULL AND lease_expires_at<=now())`
        : kind === 'customer_metrics'
          ? `(SELECT count(*) FROM customer_metric_shadow_outbox WHERE status='pending' AND lease_expires_at<=now())` : '0';
      const dead = kind === 'extraction' || kind === 'curation'
        ? `(SELECT count(*) FROM ${kind}_dead_letter WHERE dead_lettered_at>now()-interval '25 hours')`
        : kind === 'customer_metrics'
          ? `(SELECT count(*) FROM customer_metric_shadow_outbox WHERE status='dead')`
          : kind === 'platform_outbox' ? `(SELECT count(*) FROM platform_event_outbox WHERE status='dead')` : '0';
      const row = (await workerWakeQuery<{
        observed_at_ms: string; eligible_depth: string; oldest_due_age_ms: string; deferred_depth: string;
        expired_leases: string; dead_letters: string; last_cycle_success_age_ms: string | null;
      }>(db, `WITH due AS (${dueSql}) SELECT
        extract(epoch FROM now())*1000 AS observed_at_ms,
        count(*) FILTER (WHERE due_at<=now()) AS eligible_depth,
        COALESCE(GREATEST(0,extract(epoch FROM now()-min(age_at) FILTER(WHERE due_at<=now()))*1000),0) AS oldest_due_age_ms,
        count(*) FILTER (WHERE due_at>now()) AS deferred_depth,
        (${leases} + (SELECT count(*) FROM worker_execution_slot WHERE kind=$1 AND owner IS NOT NULL AND expires_at<=now())) AS expired_leases,${dead} AS dead_letters,
        (SELECT GREATEST(0,extract(epoch FROM now()-succeeded_at)*1000) FROM worker_observation_success WHERE kind=$1)
          AS last_cycle_success_age_ms FROM due`, [kind])).rows[0];
      if (!row) throw new Error('Missing shadow observation');
      observation = {event:'shadow_worker_observation',shadow_kind:kind,shadow_health:'ok',
        observed_at_ms:Number(row.observed_at_ms),eligible_depth:Number(row.eligible_depth),
        oldest_due_age_ms:Number(row.oldest_due_age_ms),deferred_depth:Number(row.deferred_depth),
        expired_leases:Number(row.expired_leases),dead_letters:Number(row.dead_letters),
        ...(kind === 'extraction' || kind === 'curation' ? {recent_dead_letter_window_ms:recentDeadLetterWindowMs} : {}),
        ...(row.last_cycle_success_age_ms === null ? {} : {last_cycle_success_age_ms:Number(row.last_cycle_success_age_ms)})};
    } catch {
      observation = {event:'shadow_worker_observation',shadow_kind:kind,shadow_health:'monitor_error',observed_at_ms:Date.now()};
    }
    output.push(observation);
    try { (options.emit ?? (record => logger.info(record)))(observation); } catch { /* External absence detects failed export. */ }
  }
  return output;
}

export function shadowDispatchCycle(kind: WorkerWakeKind, outcome: string): void {
  try {
    if (getConfig().TELEMETRY_SHADOW_ENABLED) logger.info({event:'shadow_dispatch_cycle',shadow_kind:kind,shadow_outcome:outcome});
  } catch { /* Diagnostic only. */ }
}
