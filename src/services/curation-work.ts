import type { PoolClient } from 'pg';
import { getCuratorLimits } from './curation-capacity';

export interface CurationWorkInput {
  vaultId: string;
  /** Immutable baseline mutation identity (worker action or manual operation). */
  workKey: string;
  memoryIds: readonly string[];
  segmentId?: string | null;
}

/** Call inside the baseline mutation transaction, never as an after-commit hook. */
export async function enqueueCurationWork(client: PoolClient, input: CurationWorkInput): Promise<string | null> {
  if (!input.memoryIds.length) return null;
  if (!input.workKey || input.workKey.length > 512) throw new Error('Invalid curation work identity');
  const limits = await getCuratorLimits(input.vaultId, client);
  if (!limits.curator_enabled) return null;
  // Do not mutate a different in-flight operation's queue row while holding
  // memory locks. Exact re-execution of a committed operation is already receipted
  // by its owner; this uniqueness guard also makes redundant enqueue harmless.
  const group = await client.query<{ id: string }>(
    `INSERT INTO curation_queue(vault_id,segment_id,work_key)
     SELECT $1,$2,$3 WHERE EXISTS (
       SELECT 1 FROM memories WHERE vault_id=$1 AND id=ANY($4::uuid[])
         AND status='active' AND archived_at IS NULL
     ) ON CONFLICT(vault_id,work_key) DO NOTHING RETURNING id`,
    [input.vaultId, input.segmentId ?? null, input.workKey, [...new Set(input.memoryIds)]]
  );
  if (!group.rows.length) return null;
  const id = group.rows[0].id;
  await client.query(
    `INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision)
     SELECT $1,vault_id,id,revision FROM memories
     WHERE vault_id=$2 AND id=ANY($3::uuid[]) AND status='active' AND archived_at IS NULL
     ORDER BY id`, [id, input.vaultId, [...new Set(input.memoryIds)]]
  );
  return id;
}

/** Caller owns a live queue/vault lease; absence of a target is not a memory hold. */
export async function discardObsoleteCurationTargets(client: PoolClient, queueId: string, vaultId: string): Promise<void> {
  await client.query(
    `DELETE FROM curation_queue_items item WHERE item.queue_id=$1 AND item.vault_id=$2
     AND NOT EXISTS (SELECT 1 FROM memories m WHERE m.id=item.memory_id AND m.vault_id=item.vault_id
       AND m.revision=item.revision AND m.status='active' AND m.archived_at IS NULL)`, [queueId, vaultId]
  );
}
