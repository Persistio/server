import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import { memoryValidityPredicateSql } from '../memory-validity';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('memory validity lifecycle (PostgreSQL)', () => {
  const testPool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  let archiveStaleMemories: () => Promise<void>;
  let evidenceRecallSql: () => string;
  let closeDefaultPool = async () => {};

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    ({ closePool: closeDefaultPool } = await import('../../db/client'));
    ({ archiveStaleMemories } = await import('../staleness'));
    ({ evidenceRecallSql } = await import('../../routes/recall'));

    await testPool.query(
      `INSERT INTO vaults (id, name, api_key_hash)
       VALUES ($1, $2, $3)`,
      [vaultId, `validity-${vaultId}`, crypto.randomUUID()]
    );
  });

  afterAll(async () => {
    await testPool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await testPool.end();
    await closeDefaultPool();
  });

  it('selects null and inclusive bounds while excluding expired and future rows', async () => {
    const rows = [
      ['unbounded', null, null],
      ['valid-on-boundary', '2026-05-30', '2026-05-30'],
      ['expired', null, '2026-05-29'],
      ['future', '2026-05-31', null]
    ] as const;

    for (const [data, validFrom, validUntil] of rows) {
      await testPool.query(
        `INSERT INTO memories (
           vault_id, data, subject, hash, scope, scope_key, valid_from, valid_until
         )
         VALUES ($1, $2, 'validity', $3, 'project', 'validity-project', $4::date, $5::date)`,
        [vaultId, data, crypto.randomUUID(), validFrom, validUntil]
      );
    }

    const result = await testPool.query<{ data: string }>(
      `SELECT m.data
       FROM memories m
       WHERE m.vault_id = $1
         AND ${memoryValidityPredicateSql('m', '$2')}
       ORDER BY m.data`,
      [vaultId, '2026-05-30']
    );

    expect(result.rows.map((row) => row.data)).toEqual(['unbounded', 'valid-on-boundary']);

    await expect(testPool.query(
      `INSERT INTO memories (
         vault_id, data, subject, hash, scope, scope_key, valid_from, valid_until
       ) VALUES ($1, 'inverted', 'validity', $2, 'project', 'validity-project',
                 '2026-06-01', '2026-05-01')`,
      [vaultId, crypto.randomUUID()]
    )).rejects.toMatchObject({ code: '23514' });
  });

  it('executes the evidence authority and validity recheck SQL on PostgreSQL', async () => {
    const result = await testPool.query(
      evidenceRecallSql(),
      [
        JSON.stringify([]), vaultId, 200, false, new Date().toISOString(), 'approved_only',
        new Date().toISOString().slice(0, 10), null, null, null, false, new Date().toISOString()
      ]
    );

    expect(result.rows).toEqual([]);
  });

  it('archives only rows whose end date is before the current UTC date', async () => {
    await testPool.query(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, scope_key, valid_until)
       VALUES
         ($1, 'past-cleanup', 'validity', $2, 'project', 'validity-project', (now() AT TIME ZONE 'UTC')::date - 1),
         ($1, 'today-cleanup', 'validity', $3, 'project', 'validity-project', (now() AT TIME ZONE 'UTC')::date),
         ($1, 'future-cleanup', 'validity', $4, 'project', 'validity-project', (now() AT TIME ZONE 'UTC')::date + 1),
         ($1, 'unbounded-cleanup', 'validity', $5, 'project', 'validity-project', NULL)`,
      [vaultId, crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()]
    );

    await archiveStaleMemories();

    const result = await testPool.query<{ data: string; archived: boolean }>(
      `SELECT data, archived_at IS NOT NULL AS archived
       FROM memories
       WHERE vault_id = $1 AND data LIKE '%-cleanup'
       ORDER BY data`,
      [vaultId]
    );
    expect(result.rows).toEqual([
      { data: 'future-cleanup', archived: false },
      { data: 'past-cleanup', archived: true },
      { data: 'today-cleanup', archived: false },
      { data: 'unbounded-cleanup', archived: false }
    ]);
  });
});
