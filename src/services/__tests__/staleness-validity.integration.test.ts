import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('scheduled memory staleness (PostgreSQL)', () => {
  const testPool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  let archiveStaleMemories: () => Promise<void>;
  let closeDefaultPool = async () => {};
  let decayDays: number;
  let ttlDays: number;

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    ({ closePool: closeDefaultPool } = await import('../../db/client'));
    ({ archiveStaleMemories } = await import('../staleness'));
    const { getConfig } = await import('../../config');
    decayDays = getConfig().CONFIDENCE_DECAY_INTERVAL_DAYS;
    ttlDays = getConfig().MEMORY_ARCHIVE_TTL_DAYS;
    await testPool.query(
      'INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)',
      [vaultId, `scheduled-staleness-${vaultId}`, crypto.randomUUID()]
    );
  });

  afterAll(async () => {
    await testPool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await testPool.end();
    await closeDefaultPool();
  });

  it('preserves scheduled rows and grants inactivity time from activation', async () => {
    // All fixtures were ingested long ago. Only their activation dates differ.
    const fixtures = [
      ['future', 1, 1, 0.1],
      ['future-zero-confidence', 1, 0, 0.1],
      ['activates-today', 0, 1, 0.1],
      ['within-decay-interval', -(decayDays - 1), 1, 1],
      ['after-decay-interval', -(decayDays + 1), 1, 1],
      ['low-salience-decay', -(decayDays + 1), 0.8, 0.1],
      ['within-ttl', -(ttlDays - 1), 1, 1],
      ['after-ttl', -(ttlDays + 1), 1, 1],
      ['unbounded-old', null, 1, 1]
    ] as const;
    for (const [data, activationDays, confidence, salience] of fixtures) {
      await testPool.query(
        `INSERT INTO memories (
           vault_id, data, subject, hash, scope, scope_key, confidence, salience,
           valid_from, created_at, updated_at, status
         ) VALUES (
           $1, $2, 'validity', $3, 'project', 'staleness-project', $4, $5,
           (now() AT TIME ZONE 'UTC')::date + $6::integer,
           now() - (($7::integer + 1000)::text || ' days')::interval,
           now() - (($7::integer + 1000)::text || ' days')::interval,
           CASE WHEN $4::double precision = 0 THEN 'candidate' ELSE 'active' END
         )`,
        [vaultId, data, crypto.randomUUID(), confidence, salience, activationDays, ttlDays]
      );
    }

    await archiveStaleMemories();

    const result = await testPool.query<{ data: string; confidence: number; archived: boolean; status: string }>(
      `SELECT data, confidence, archived_at IS NOT NULL AS archived, status
       FROM memories WHERE vault_id = $1 ORDER BY data`,
      [vaultId]
    );
    const rows = new Map(result.rows.map((row) => [row.data, row]));
    for (const name of ['future', 'activates-today', 'within-decay-interval']) {
      expect(rows.get(name)).toMatchObject({ archived: false, confidence: 1 });
    }
    expect(rows.get('future-zero-confidence')).toMatchObject({ archived: false, confidence: 0 });
    expect(rows.get('after-decay-interval')).toMatchObject({ archived: false, confidence: 0, status: 'needs_review' });
    expect(rows.get('low-salience-decay')).toMatchObject({ archived: true, confidence: 0, status: 'needs_review' });
    expect(rows.get('within-ttl')).toMatchObject({ archived: false });
    expect(rows.get('after-ttl')).toMatchObject({ archived: true });
    expect(rows.get('unbounded-old')).toMatchObject({ archived: true });
  });
});
