import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('worker-lease');
import type { PoolClient } from 'pg';

import { withTransaction } from '../db/client';

export type WorkerQueueKind = 'extraction' | 'curation';

interface QueueCapability {
  queueId: string;
  claimToken: string;
  workerId: string;
}
export type WorkerLease = QueueCapability & (
  { queueKind: 'extraction' } |
  { queueKind: 'curation'; vaultId: string; vaultClaimToken: string }
);

export class StaleWorkerLeaseError extends Error {
  constructor(readonly lease: WorkerLease) {
    super(`Worker no longer owns ${lease?.queueKind} queue row ${lease?.queueId}`);
    this.name = 'StaleWorkerLeaseError';
  }
}

function validateLease(lease: WorkerLease): void {
  const uuid = (value: unknown) => typeof value === 'string' && /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(value);
  if (!lease || !['extraction', 'curation'].includes(lease.queueKind)
    || !uuid(lease.queueId) || !uuid(lease.claimToken) || typeof lease.workerId !== 'string' || !lease.workerId
    || lease.queueKind === 'curation' && (!uuid(lease.vaultId) || !uuid(lease.vaultClaimToken))) {
    throw new StaleWorkerLeaseError(lease);
  }
}

/** Locks stay held through COMMIT, including after the queue row is deleted.
 * Capture SQL timestamp text (not JS milliseconds) so the final check retains
 * the exact deadline. A heartbeat cannot renew these locked rows concurrently. */
export async function assertCurrentWorkerLease(client: PoolClient, lease: WorkerLease): Promise<{ assertUnexpired(): Promise<void> }> {
  validateLease(lease);
  const table = lease.queueKind === 'extraction' ? 'extraction_queue' : 'curation_queue';
  const result = await client.query<{ deadline: string }>(
    `SELECT lease_expires_at::text AS deadline
     FROM ${table}
     WHERE id = $1
       AND claim_token = $2
       AND claimed_by = $3
       ${lease.queueKind === 'curation' ? 'AND vault_id = $4' : ''}
     FOR UPDATE`,
    [lease.queueId, lease.claimToken, lease.workerId, ...(lease.queueKind === 'curation' ? [lease.vaultId] : [])]
  );
  if (result.rowCount !== 1) throw new StaleWorkerLeaseError(lease);

  let vaultDeadline: string | null = null;
  if (lease.queueKind === 'curation') {
    const vaultResult = await client.query<{ deadline: string }>(
      `SELECT curator_claimed_until::text AS deadline
       FROM vault_curation_state
       WHERE vault_id = $1
         AND curator_claim_token = $2
         AND curator_claimed_by = $3
       FOR UPDATE`,
      [lease.vaultId, lease.vaultClaimToken, lease.workerId]
    );
    if (vaultResult.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
    vaultDeadline = vaultResult.rows[0].deadline;
    if (!vaultDeadline) throw new StaleWorkerLeaseError(lease);
  }
  const deadline = result.rows[0].deadline;
  const fence = { async assertUnexpired() {
    const checked = await client.query<{ live: boolean }>(
      `SELECT $1::timestamptz > clock_timestamp()
         AND ($2::timestamptz IS NULL OR $2::timestamptz > clock_timestamp()) AS live`,
      [deadline, vaultDeadline]);
    if (checked.rows[0]?.live !== true) throw new StaleWorkerLeaseError(lease);
  } };
  await fence.assertUnexpired();
  return fence;
}

/** Every worker-owned SQL stage, including final queue deletion, uses this owner. */
export async function withWorkerLeaseTransaction<T>(lease: WorkerLease, work: (client: PoolClient) => Promise<T>): Promise<T> {
  return withTransaction(async client => {
    const fence = await assertCurrentWorkerLease(client, lease);
    const result = await work(client);
    await fence.assertUnexpired();
    return result;
  });
}

export async function recordWorkerAction(
  client: PoolClient,
  lease: WorkerLease,
  actionKey: string
): Promise<boolean> {
  const result = await client.query(
    `INSERT INTO worker_action_receipts (queue_kind, queue_id, action_key, claim_token)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (queue_kind, queue_id, action_key) DO NOTHING
     RETURNING queue_id`,
    [lease.queueKind, lease.queueId, actionKey, lease.claimToken]
  );
  return result.rowCount === 1;
}

export interface LeaseHeartbeat {
  lost: boolean;
  stop(): Promise<void>;
  renewNow(): Promise<boolean>;
}

export function startWorkerLeaseHeartbeat(
  lease: WorkerLease,
  leaseMs = 10 * 60_000,
  intervalMs = Math.max(1_000, Math.floor(leaseMs / 3))
): LeaseHeartbeat {
  let stopped = false;
  let lost = false;
  let active: Promise<boolean> | null = null;

  const renewNow = async (): Promise<boolean> => {
    if (stopped || lost) return false;
    if (active) return active;
    active = renewWorkerLease(lease, leaseMs).then((renewed) => {
      if (!renewed) lost = true;
      return renewed;
    }).catch((error) => {
      // A transient database failure is not proof of takeover. The final
      // transaction still performs the authoritative, locking fence check.
      operationalLog.warn(JSON.stringify({
        level: 40,
        msg: 'worker lease renewal failed',
        queue: lease.queueKind,
        queue_id: lease.queueId,
        error: error instanceof Error ? error.message : String(error)
      }));
      return false;
    }).finally(() => {
      active = null;
    });
    return active;
  };

  const timer = setInterval(() => void renewNow(), intervalMs);
  timer.unref?.();

  return {
    get lost() { return lost; },
    renewNow,
    async stop() {
      stopped = true;
      clearInterval(timer);
      if (active) await active;
    }
  };
}

export async function renewWorkerLease(lease: WorkerLease, leaseMs: number): Promise<boolean> {
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0) throw new Error('Invalid worker lease duration');
  try {
    return await withWorkerLeaseTransaction(lease, async (client) => {
      const table = lease.queueKind === 'extraction' ? 'extraction_queue' : 'curation_queue';
      const result = await client.query(
        `UPDATE ${table}
         SET lease_expires_at = clock_timestamp() + ($4::bigint * interval '1 millisecond')
         WHERE id = $1
           AND claim_token = $2
           AND claimed_by = $3
           AND lease_expires_at > clock_timestamp()`,
        [lease.queueId, lease.claimToken, lease.workerId, leaseMs]
      );
      if (result.rowCount !== 1) throw new StaleWorkerLeaseError(lease);

      if (lease.queueKind === 'curation') {
        const vaultResult = await client.query(
          `UPDATE vault_curation_state
           SET curator_claimed_until = clock_timestamp() + ($4::bigint * interval '1 millisecond'),
               updated_at = now()
           WHERE vault_id = $1
             AND curator_claim_token = $2
             AND curator_claimed_by = $3
             AND curator_claimed_until > clock_timestamp()`,
          [lease.vaultId, lease.vaultClaimToken, lease.workerId, leaseMs]
        );
        if (vaultResult.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
      }
      return true;
    });
  } catch (error) {
    if (error instanceof StaleWorkerLeaseError) return false;
    throw error;
  }
}

export interface WorkerReleaseUpdates {
  availableAt?: Date;
  incrementRetry?: boolean;
  lastError?: string;
}

export async function releaseWorkerLease(lease: WorkerLease, updates: WorkerReleaseUpdates = {}): Promise<boolean> {
  try {
    return await withWorkerLeaseTransaction(lease, async client => {
      await releaseWorkerLeaseInTransaction(client, lease, updates);
      return true;
    });
  } catch (error) {
    if (error instanceof StaleWorkerLeaseError) return false;
    throw error;
  }
}

export async function releaseWorkerLeaseInTransaction(client: PoolClient, lease: WorkerLease, updates: WorkerReleaseUpdates = {}): Promise<void> {
  const fence = await assertCurrentWorkerLease(client, lease);
  const table = lease.queueKind === 'extraction' ? 'extraction_queue' : 'curation_queue';
  const result = await client.query(
    `UPDATE ${table}
     SET claimed_at = NULL,
         claimed_by = NULL,
         claim_token = NULL,
         lease_expires_at = NULL,
         retry_count = retry_count + $4,
         last_error = $5,
         available_at = COALESCE($6, available_at)
     WHERE id = $1
       AND claim_token = $2
       AND claimed_by = $3`,
    [
      lease.queueId,
      lease.claimToken,
      lease.workerId,
      updates.incrementRetry ? 1 : 0,
      updates.lastError ?? null,
      updates.availableAt?.toISOString() ?? null
    ]
  );
  if (result.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
  await fence.assertUnexpired();
}
