import crypto from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { Pool } from 'pg';

const { settings } = vi.hoisted(() => ({ settings: { MEMORY_ARCHIVE_TTL_DAYS: 0 } }));
vi.mock('../../config', async importOriginal => {
  const actual = await importOriginal<typeof import('../../config')>();
  return { ...actual, getConfig: () => ({ ...actual.getConfig(), ...settings }) };
});
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('historical knowledge and opt-in retention (PostgreSQL)', () => {
  const testPool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  let archiveStaleMemories: () => Promise<void>;
  let closeDefaultPool = async () => {};

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    ({ closePool: closeDefaultPool } = await import('../../db/client'));
    ({ archiveStaleMemories } = await import('../staleness'));
    await testPool.query('INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)',
      [vaultId, `retention-${vaultId}`, crypto.randomUUID()]);
    for (const [name, from, until, updated, recalled] of [
      ['historical', -500, -400, -500, null],
      ['future', 5, null, -500, null],
      ['recent-correction', null, null, -1, -500],
      ['recent-recall', null, null, -500, -1],
      ['old', null, null, -500, null]
    ] as const) {
      await testPool.query(`INSERT INTO memories
        (vault_id, data, subject, hash, scope, confidence, salience, valid_from, valid_until,
         created_at, updated_at, last_recalled, status)
        VALUES ($1, $2, 'retention', $3, 'global', 0.8, 0.5,
          (now() AT TIME ZONE 'UTC')::date + $4::integer,
          (now() AT TIME ZONE 'UTC')::date + $5::integer,
          now() - interval '500 days', now() + $6::integer * interval '1 day',
          now() + $7::integer * interval '1 day', 'active')`,
      [vaultId, name, crypto.randomUUID(), from, until, updated, recalled]);
    }
  });

  afterAll(async () => {
    await testPool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await testPool.end();
    await closeDefaultPool();
  });

  it('keeps history/confidence by default and uses actual activity under explicit retention', async () => {
    settings.MEMORY_ARCHIVE_TTL_DAYS = 0;
    await archiveStaleMemories();
    const unchanged = await testPool.query('SELECT status, confidence, archived_at FROM memories WHERE vault_id = $1', [vaultId]);
    expect(unchanged.rows).toHaveLength(5);
    for (const row of unchanged.rows) expect(row).toMatchObject({ status: 'active', confidence: 0.8, archived_at: null });
    settings.MEMORY_ARCHIVE_TTL_DAYS = 90;
    await archiveStaleMemories();
    const retained = await testPool.query('SELECT data FROM memories WHERE vault_id = $1 AND archived_at IS NULL ORDER BY data', [vaultId]);
    expect(retained.rows.map(row => row.data)).toEqual(['future', 'recent-correction', 'recent-recall']);
    const all = await testPool.query('SELECT confidence FROM memories WHERE vault_id = $1', [vaultId]);
    expect(all.rows.every(row => row.confidence === 0.8)).toBe(true);
  });
});
