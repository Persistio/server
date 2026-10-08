import { recordShadowCycleSuccess } from '../services/shadow-observability';
import type { Pool, PoolClient } from 'pg';
import { completeWorkerWake } from '../services/worker-wake-store';
import type { WorkerWake } from '../services/worker-wake-types';

export type Admission = { status: 'acquired'; previousOwner: string | null } | { status: 'busy' } | { status: 'stale' };
export class WorkerExecutionStore {
  constructor(private readonly pool: Pick<Pool, 'connect' | 'query'>, private readonly requirePollerDrained = false) {}

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    let broken: Error | undefined;
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try { await client.query('ROLLBACK'); } catch { broken = new Error('Execution transaction cleanup failed'); }
      throw error;
    } finally { client.release(broken); }
  }

  async acquire(owner: string, wake: WorkerWake): Promise<Admission> {
    return this.transaction(async client => {
      // Defence in depth during the staged IaC handoff. Actual revision drainage
      // remains mandatory: SQL absence alone cannot prevent an old revision restart.
      if (this.requirePollerDrained) {
        const legacy = await client.query(`SELECT 1 FROM pg_stat_activity
          WHERE datname=current_database() AND (application_name LIKE 'persistio:worker:%'
            OR application_name LIKE 'persistio:combined:%') LIMIT 1`);
        if (legacy.rowCount) return { status: 'busy' };
      }
      const slot = (await client.query<{ owner: string | null; live: boolean }>(
        'SELECT owner, expires_at > clock_timestamp() AS live FROM worker_execution_slot WHERE id=1 FOR UPDATE'
      )).rows[0];
      if (!slot) throw new Error('Execution slot migration missing');
      if (slot.owner && slot.live) return { status: 'busy' };
      const current = await client.query(
        'SELECT 1 FROM worker_wake_intents WHERE kind=$1 AND active_token=$2 AND active_generation=$3::bigint FOR UPDATE',
        [wake.kind, wake.token, wake.generation]);
      if (!current.rowCount) return { status: 'stale' };
      await client.query(
        `UPDATE worker_execution_slot SET owner=$1,kind=$2,token=$3,generation=$4::bigint,
         expires_at=clock_timestamp()+interval '90 seconds' WHERE id=1`,
        [owner, wake.kind, wake.token, wake.generation]);
      return { status: 'acquired', previousOwner: slot.owner };
    });
  }

  async renew(owner: string): Promise<boolean> {
    const result = await this.pool.query(
      `UPDATE worker_execution_slot s SET expires_at=clock_timestamp()+interval '90 seconds'
       WHERE id=1 AND owner=$1 AND expires_at>clock_timestamp()
         AND EXISTS(SELECT 1 FROM worker_wake_intents w WHERE w.kind=s.kind
           AND w.active_token=s.token AND w.active_generation=s.generation)`, [owner]);
    return result.rowCount === 1;
  }

  /** A terminated JS thread is not proof an already submitted SQL statement stopped. */
  async clearBackends(owner: string): Promise<void> {
    const application = 'persistio:req:' + owner;
    await this.pool.query(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
       WHERE datname=current_database() AND usename=current_user AND application_name=$1
         AND pid<>pg_backend_pid()`, [application]);
    // Termination signals are asynchronous. Verify before releasing admission.
    for (let attempt = 0; attempt < 20; attempt++) {
      const result = await this.pool.query(
        `SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
         AND usename=current_user AND application_name=$1`, [application]);
      if (!result.rowCount) return;
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    throw new Error('Execution SQL did not settle');
  }

  async finish(owner: string, wake: WorkerWake, success: boolean): Promise<boolean> {
    return this.transaction(async client => {
      const slot = await client.query(
        `SELECT 1 FROM worker_execution_slot WHERE id=1 AND owner=$1 AND kind=$2
         AND token=$3 AND generation=$4::bigint AND expires_at>clock_timestamp() FOR UPDATE`,
        [owner, wake.kind, wake.token, wake.generation]);
      if (!slot.rowCount) return false;
      const acknowledged = success ? await completeWorkerWake(client, wake) : false;
      if (acknowledged) await recordShadowCycleSuccess(client, wake.kind);
      await client.query('UPDATE worker_execution_slot SET owner=NULL,kind=NULL,token=NULL,generation=NULL,expires_at=NULL WHERE id=1 AND owner=$1', [owner]);
      return acknowledged;
    });
  }
}
