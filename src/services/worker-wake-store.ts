import { workerWakeQuery } from './worker-wake-query';
import type { Pool, PoolClient } from 'pg';
import { WORKER_WAKE_KINDS, workerWakeSchema, type WorkerWake, type WorkerWakeKind } from './worker-wake-types';

export interface WakeRequest { kind: WorkerWakeKind; dueAt: Date }
type QueryClient = Pick<PoolClient, 'query'>;

/** Call once at the END of the business transaction. Never pass the global pool:
 * all business writes and these requests must commit or roll back together. */
export async function requestWorkerWakes(
  client: PoolClient, requests: readonly WakeRequest[], enabled = false
): Promise<void> {
  if (!enabled) return;
  const coalesced = new Map<WorkerWakeKind, Date>();
  for (const { kind, dueAt } of requests) {
    if (!WORKER_WAKE_KINDS.includes(kind) || !Number.isFinite(dueAt.getTime())) throw new Error('Invalid wake request');
    if (!coalesced.has(kind) || dueAt < coalesced.get(kind)!) coalesced.set(kind, dueAt);
  }
  for (const kind of [...coalesced.keys()].sort()) {
    await client.query(
      `INSERT INTO worker_wake_intents(kind, requested_generation, pending_due_at)
       VALUES ($1, 1, $2)
       ON CONFLICT(kind) DO UPDATE SET
         requested_generation = worker_wake_intents.requested_generation + 1,
         pending_due_at = LEAST(worker_wake_intents.pending_due_at, EXCLUDED.pending_due_at)`,
      [kind, coalesced.get(kind)]
    );
  }
}

export interface WakeReservation {
  wake: WorkerWake;
  owner: string;
  dueAt: Date;
  dispatched: boolean;
}

interface ReservedRow {
  kind: WorkerWakeKind;
  active_generation: string;
  active_token: string;
  active_due_at: Date;
  dispatch_owner: string;
  dispatched: boolean;
}

export class WorkerWakeStore {
  constructor(private readonly db: Pick<Pool, 'connect'>) {}

  /** Reconciliation does not manufacture new generations while one is already pending.
   * It can re-arm acknowledged work and move an existing due time earlier. */
  async reconcile(request: WakeRequest): Promise<void> {
    await workerWakeQuery(this.db,
      `INSERT INTO worker_wake_intents(kind, requested_generation, pending_due_at)
       VALUES ($1, 1, $2)
       ON CONFLICT(kind) DO UPDATE SET
         requested_generation = CASE WHEN worker_wake_intents.pending_due_at IS NULL
           THEN worker_wake_intents.requested_generation + 1 ELSE worker_wake_intents.requested_generation END,
         pending_due_at = LEAST(worker_wake_intents.pending_due_at, EXCLUDED.pending_due_at)`,
      [request.kind, request.dueAt]
    );
  }

  async reserve(kind: WorkerWakeKind, allowance: number, cycleMs: number): Promise<WakeReservation | null> {
    if (!Number.isSafeInteger(allowance) || allowance < 0 || allowance > 1_000_000 ||
        !Number.isSafeInteger(cycleMs) || cycleMs < 1000 || cycleMs > 86_400_000) throw new Error('Invalid wake cycle bounds');
    const result = await workerWakeQuery<ReservedRow>(this.db,
      `UPDATE worker_wake_intents SET
         active_generation = COALESCE(active_generation, requested_generation),
         active_token = COALESCE(active_token, gen_random_uuid()),
         active_due_at = COALESCE(active_due_at, pending_due_at),
         pending_due_at = CASE WHEN active_generation IS NULL THEN NULL ELSE pending_due_at END,
         allowance = CASE WHEN active_generation IS NULL AND kind = 'contradiction' THEN $2 ELSE allowance END,
         cycle_interval_ms = CASE WHEN active_generation IS NULL AND kind = 'contradiction' THEN $3 ELSE cycle_interval_ms END,
         allowance_used = CASE WHEN active_generation IS NULL THEN 0 ELSE allowance_used END,
         next_cycle_at = CASE WHEN active_generation IS NULL AND kind = 'contradiction'
           THEN clock_timestamp() + ($3::bigint * interval '1 millisecond') ELSE next_cycle_at END,
         dispatch_owner = gen_random_uuid(),
         dispatch_expires_at = clock_timestamp() + interval '60 seconds'
       WHERE kind = $1
         AND (dispatch_expires_at IS NULL OR dispatch_expires_at <= clock_timestamp())
         AND next_attempt_at <= clock_timestamp()
         AND (active_generation IS NOT NULL OR
           (pending_due_at <= clock_timestamp() AND next_cycle_at <= clock_timestamp()))
       RETURNING kind, active_generation::text, active_token, active_due_at, dispatch_owner, dispatched`,
      [kind, allowance, cycleMs]
    );
    const row = result.rows[0];
    return row ? {
      wake: { version: 1, kind: row.kind, token: row.active_token, generation: row.active_generation },
      owner: row.dispatch_owner, dueAt: row.active_due_at, dispatched: row.dispatched
    } : null;
  }

  async dispatched(reservation: WakeReservation): Promise<boolean> {
    return this.settle(reservation, `dispatched = true, attempts = 0,
      next_attempt_at = clock_timestamp() + interval '60 seconds'`);
  }

  async failed(reservation: WakeReservation): Promise<boolean> {
    return this.settle(reservation, `attempts = LEAST(attempts + 1, 20),
      next_attempt_at = clock_timestamp() + (LEAST(300, power(2, LEAST(attempts + 1, 9))) * interval '1 second')`);
  }

  /** Positive transport absence only. Cycle/allowance survive task-name replacement. */
  async rotate(reservation: WakeReservation): Promise<boolean> {
    return this.settle(reservation, `active_token = gen_random_uuid(), dispatched = false,
      next_attempt_at = clock_timestamp(), attempts = 0`);
  }

  private async settle(reservation: WakeReservation, assignments: string): Promise<boolean> {
    const result = await workerWakeQuery(this.db,
      `UPDATE worker_wake_intents SET ${assignments}, dispatch_owner = NULL, dispatch_expires_at = NULL
       WHERE kind = $1 AND active_generation = $2::bigint AND active_token = $3
         AND dispatch_owner = $4 AND dispatch_expires_at > clock_timestamp()`,
      [reservation.wake.kind, reservation.wake.generation, reservation.wake.token, reservation.owner]
    );
    return result.rowCount === 1;
  }
}

/** Future request host calls this only after admitted work is fully settled.
 * Passing its transaction client permits an atomic final durable-state update. */
export async function completeWorkerWake(client: QueryClient, value: WorkerWake): Promise<boolean> {
  const wake = workerWakeSchema.parse(value);
  const result = await client.query(
    `UPDATE worker_wake_intents SET completed_generation = active_generation,
       active_generation = NULL, active_token = NULL, active_due_at = NULL,
       dispatched = false, dispatch_owner = NULL, dispatch_expires_at = NULL,
       next_cycle_at = CASE WHEN kind = 'contradiction'
         THEN clock_timestamp() + (cycle_interval_ms::bigint * interval '1 millisecond') ELSE next_cycle_at END,
       cycle_interval_ms = 0, allowance = 0, allowance_used = 0, attempts = 0, next_attempt_at = clock_timestamp()
     WHERE kind = $1 AND active_generation = $2::bigint AND active_token = $3`,
    [wake.kind, wake.generation, wake.token]
  );
  return result.rowCount === 1;
}

/** Admission is spent before a provider call. A crash may under-spend, never replenish.
 * The caller must retain the existing vault/row fences and settle before completion. */
export async function admitContradictionArbitration(client: QueryClient, value: WorkerWake): Promise<boolean> {
  const wake = workerWakeSchema.parse(value);
  if (wake.kind !== 'contradiction') return false;
  const result = await client.query(
    `UPDATE worker_wake_intents SET allowance_used = allowance_used + 1
     WHERE kind = 'contradiction' AND active_generation = $1::bigint AND active_token = $2
       AND allowance_used < allowance`, [wake.generation, wake.token]
  );
  return result.rowCount === 1;
}
