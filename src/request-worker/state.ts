import type { Pool } from 'pg';
import { z } from 'zod';
import { assertExecutionLive, executionSignal, invalidateExecution } from './context';

let saved: Record<string, unknown> | undefined;
let persist: ((state: Record<string, unknown>) => Promise<void>) | undefined;
const snapshots = new Map<string, () => unknown>();
let checkpoint = Promise.resolve();

/** Load before importing stateful business services. The singleton slot serializes owners. */
export async function initializeExecutionState(pool: Pool, owner: string): Promise<void> {
  assertExecutionLive();
  const result = await pool.query('SELECT runtime_state FROM worker_execution_slot WHERE id=1 AND owner=$1 AND expires_at>clock_timestamp()', [owner]);
  if (result.rowCount !== 1) throw new Error('Execution ownership missing');
  saved = z.record(z.unknown()).parse(result.rows[0].runtime_state);
  persist = async state => {
    const update = await pool.query(
      `UPDATE worker_execution_slot SET runtime_state=$2::jsonb WHERE id=1 AND owner=$1
       AND expires_at>clock_timestamp() AND EXISTS(SELECT 1 FROM worker_wake_intents w
         WHERE w.kind=worker_execution_slot.kind AND w.active_token=worker_execution_slot.token
           AND w.active_generation=worker_execution_slot.generation)`, [owner, JSON.stringify(state)]);
    if (update.rowCount !== 1) throw new Error('Execution checkpoint lost ownership');
  };
}

/** Business services retain their existing state machines; only request mode restores them. */
export function registerExecutionState(key: string, restore: (value: unknown) => void, snapshot: () => unknown): void {
  if (!executionSignal()) return;
  if (!saved || !persist || snapshots.has(key)) throw new Error('Invalid execution state registration');
  if (Object.hasOwn(saved, key)) restore(saved[key]);
  snapshots.set(key, snapshot);
}

/** Pre-send reservation checkpoints are required, not best-effort telemetry. */
export async function checkpointExecutionState(): Promise<void> {
  if (!executionSignal()) return;
  checkpoint = checkpoint.then(async () => {
    assertExecutionLive();
    if (!saved || !persist) throw new Error('Execution state not initialized');
    const next = { ...saved };
    for (const [key, snapshot] of snapshots) next[key] = snapshot();
    await persist(next);
    saved = next;
  }).catch(error => { invalidateExecution(); throw error; });
  await checkpoint;
}
