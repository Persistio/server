import { getConfig } from '../config';
import { query, withTransaction } from '../db/client';
import { recordMemoryCountDelta } from './usage';

/** Explicit inactivity retention, not a judgement about whether a fact is true. */
export async function archiveStaleMemories() {
  const { MEMORY_ARCHIVE_TTL_DAYS: ttlDays } = getConfig();
  if (ttlDays === 0) return;
  const stale = `archived_at IS NULL AND GREATEST(
    valid_from::timestamp AT TIME ZONE 'UTC', last_recalled, updated_at, created_at
  ) < now() - ($1::text || ' days')::interval`;
  const vaults = await query<{ vault_id: string }>(
    `SELECT DISTINCT vault_id FROM memories WHERE ${stale} ORDER BY vault_id`, [ttlDays]);
  for (const { vault_id: vaultId } of vaults.rows) {
    // Same vault-before-memory lock order as baseline writers and Curator.
    // Re-evaluate inactivity after acquiring the lock, not from discovery time.
    const result = await withTransaction(async client => {
      const vault = (await client.query<{ account_id: string | null }>(
        'SELECT account_id FROM vaults WHERE id=$1 FOR NO KEY UPDATE', [vaultId])).rows[0];
      if (!vault) return null;
      const archived = await client.query(
        `UPDATE memories SET archived_at=now(),updated_at=now() WHERE vault_id=$2 AND ${stale}`,
        [ttlDays, vaultId]);
      return { accountId: vault.account_id, count: archived.rowCount ?? 0 };
    });
    if (result?.count) recordMemoryCountDelta(vaultId, result.accountId, -result.count, 'extraction_worker');
  }
}
