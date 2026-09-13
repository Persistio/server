import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { memoryCapacityPredicateSql } from '../memory-capacity';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('memory capacity and inventory (PostgreSQL)', () => {
  const testPool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  let canCreateMemory: typeof import('../usage').canCreateMemory;
  let enforceMemoryCreationLimit: typeof import('../usage').enforceMemoryCreationLimit;
  let getVaultStats: typeof import('../vault-stats').getVaultStats;
  let closeDefaultPool = async () => {};

  beforeAll(async () => {
    const connectionUrl = new URL(databaseUrl!);
    connectionUrl.searchParams.set('options', '-c timezone=Pacific/Kiritimati');
    process.env.DATABASE_URL = connectionUrl.toString();
    ({ closePool: closeDefaultPool } = await import('../../db/client'));
    ({ canCreateMemory, enforceMemoryCreationLimit } = await import('../usage'));
    ({ getVaultStats } = await import('../vault-stats'));

    await testPool.query(
      `INSERT INTO vaults (id, name, api_key_hash, rate_limit_override)
       VALUES ($1, 'capacity-regression', $2, '{"memories_max":12}')`,
      [vaultId, crypto.randomUUID()]
    );
    // Every retained status reserves capacity, including historical and future memories.
    // Only archival releases storage; applicability is not retention.
    for (const status of ['active', 'contradicted', 'superseded']) {
      await testPool.query(
        `INSERT INTO memories (vault_id, data, subject, hash, status, scope, scope_key, valid_from, valid_until)
         SELECT $1, $2 || '-' || kind, 'capacity', $3 || '-' || kind, $2, 'project', 'capacity-test',
           CASE WHEN kind = 'future' THEN (now() AT TIME ZONE 'UTC')::date + 1 END,
           CASE WHEN kind = 'expired' THEN (now() AT TIME ZONE 'UTC')::date - 1
                WHEN kind = 'today' THEN (now() AT TIME ZONE 'UTC')::date END
         FROM unnest(ARRAY['expired', 'today', 'future', 'unbounded', 'archived']) AS kind`,
        [vaultId, status, crypto.randomUUID()]
      );
    }
    await testPool.query(`UPDATE memories SET archived_at = now() WHERE vault_id = $1 AND data LIKE '%-archived'`, [vaultId]);
  });

  afterAll(async () => {
    await testPool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await testPool.end();
    await closeDefaultPool();
  });

  it('counts all retained dates regardless of the connection timezone', async () => {
    const client = await testPool.connect();
    try {
      for (const timezone of ['Pacific/Kiritimati', 'Etc/GMT+12']) {
        await client.query('SELECT set_config(\'TimeZone\', $1, false)', [timezone]);
        const result = await client.query<{ data: string }>(
          `SELECT m.data FROM memories m
           WHERE m.vault_id = $1 AND m.status = 'active' AND ${memoryCapacityPredicateSql('m')}
           ORDER BY m.data`,
          [vaultId]
        );
        expect(result.rows.map((row) => row.data)).toEqual(['active-expired', 'active-future', 'active-today', 'active-unbounded']);
      }
    } finally {
      client.release();
    }
  });

  it('reports the same capacity and override used by both admission paths while preserving inventory', async () => {
    expect((await getVaultStats(vaultId))?.memories).toEqual({
      active: 4, contradicted: 4,
      superseded: 4, archived: 3, capacity_used: 12, limit: 12
    });
    await expect(canCreateMemory(vaultId)).resolves.toBe(false);
    await expect(enforceMemoryCreationLimit(vaultId)).rejects.toMatchObject({ name: 'QuotaExceededError' });

    await testPool.query(`UPDATE vaults SET rate_limit_override = '{"memories_max":13}' WHERE id = $1`, [vaultId]);
    expect((await getVaultStats(vaultId))?.memories).toMatchObject({ active: 4, capacity_used: 12, limit: 13 });
    await expect(canCreateMemory(vaultId)).resolves.toBe(true);
    const client = await testPool.connect();
    try {
      await client.query('BEGIN');
      await enforceMemoryCreationLimit(vaultId, 'api', client);
      await client.query(
        `INSERT INTO memories (vault_id, data, subject, hash, scope, scope_key)
         VALUES ($1, 'new-slot', 'capacity', $2, 'project', 'capacity-test')`,
        [vaultId, crypto.randomUUID()]
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    expect((await getVaultStats(vaultId))?.memories.capacity_used).toBe(13);
    await expect(canCreateMemory(vaultId)).resolves.toBe(false);
  });
});
