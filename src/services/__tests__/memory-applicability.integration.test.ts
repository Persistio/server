import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import { memoryApplicabilityPredicateSql, memoryEligibilityPredicateSql, recallContextSchema } from '../memory-applicability';
import { ingestSchema } from '../../routes/ingest';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('memory applicability boundary (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultA = crypto.randomUUID();
  const vaultB = crypto.randomUUID();

  beforeAll(async () => {
    const migration = await pool.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '047_memory_applicability.sql') AS applied`
    );
    if (!migration.rows[0]?.applied) throw new Error('Test database must be migrated through 047_memory_applicability.sql');
    await pool.query(
      `INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, 'applicability-a', $3), ($2, 'applicability-b', $4)`,
      [vaultA, vaultB, crypto.randomUUID(), crypto.randomUUID()]
    );
  });

  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [[vaultA, vaultB]]);
    await pool.end();
  });

  it('round-trips accepted ingest session identities through persisted scope and recall', async () => {
    for (const supplied of [' session-roundtrip ', 's'.repeat(512), 'é-session', 'session-😀']) {
      const ingest = ingestSchema.parse({ session_id: supplied, chunks: [{ role: 'user', content: 'fact', timestamp: '2026-05-12T16:00:00Z' }] });
      const context = recallContextSchema.parse({ session_id: supplied });
      const inserted = await pool.query<{ id: string }>(
        `INSERT INTO memories (vault_id, data, subject, hash, type, scope, scope_key, status)
         VALUES ($1, 'roundtrip', 'scope', $2, 'system_fact', 'session', $3, 'active') RETURNING id`,
        [vaultA, crypto.randomUUID(), ingest.session_id]
      );
      const result = await pool.query(
        `SELECT m.id FROM memories m WHERE m.id = $1 AND m.vault_id = $2
         AND ${memoryApplicabilityPredicateSql('m', '$3', '$4', '$5', '$6')}`,
        [inserted.rows[0].id, vaultA, context.session_id, null, null, false]
      );
      expect(result.rows).toEqual(inserted.rows);
      await pool.query('DELETE FROM memories WHERE id = $1', [inserted.rows[0].id]);
    }
  });

  it('requires bindings for active non-global writes and for later promotion', async () => {
    await expect(pool.query(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, status)
       VALUES ($1, 'unbound active', 'scope', $2, 'project', 'active')`,
      [vaultA, crypto.randomUUID()]
    )).rejects.toMatchObject({ code: 'P0001' });

    const quarantined = await pool.query<{ id: string }>(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, status)
       VALUES ($1, 'unbound review', 'scope', $2, 'project', 'needs_review') RETURNING id`,
      [vaultA, crypto.randomUUID()]
    );
    await expect(pool.query(
      `UPDATE memories SET status = 'active' WHERE id = $1`,
      [quarantined.rows[0].id]
    )).rejects.toMatchObject({ code: 'P0001' });
  });

  it('isolates entity aliases by the same exact scope binding', async () => {
    await pool.query(
      `INSERT INTO entity_aliases (vault_id, alias, canonical, scope, scope_key)
       VALUES
         ($1, 'service', 'project-a-service', 'project', 'project-a'),
         ($1, 'service', 'project-b-service', 'project', 'project-b'),
         ($1, 'legacy', 'legacy-service', NULL, NULL)`,
      [vaultA]
    );

    const aliases = await pool.query<{ canonical: string }>(
      `SELECT canonical
       FROM entity_aliases
       WHERE vault_id = $1
         AND alias = 'service'
         AND scope = 'project'
         AND scope_key IS NOT DISTINCT FROM $2::text`,
      [vaultA, 'project-a']
    );
    expect(aliases.rows.map((row) => row.canonical)).toEqual(['project-a-service']);

    await expect(pool.query(
      `INSERT INTO entity_aliases (vault_id, alias, canonical, scope, scope_key)
       VALUES ($1, 'invalid', 'invalid', 'global', 'project-a')`,
      [vaultA]
    )).rejects.toMatchObject({ code: '23514' });
    await expect(pool.query(
      `INSERT INTO entity_aliases (vault_id, alias, canonical, scope, scope_key)
       VALUES ($1, 'unbound', 'unbound', 'project', NULL)`,
      [vaultA]
    )).rejects.toMatchObject({ code: '23514' });
  });

  it('returns only exact-context, same-vault, eligible rows and gates global rules', async () => {
    const rows = [
      ['project-a', vaultA, 'system_fact', 'project', 'project-a', 'low', 0.9, null],
      ['project-b', vaultA, 'system_fact', 'project', 'project-b', 'low', 0.9, null],
      ['other-vault', vaultB, 'system_fact', 'project', 'project-a', 'low', 0.9, null],
      ['global-fact', vaultA, 'system_fact', 'global', null, 'low', 0.9, null],
      ['global-rule', vaultA, 'user_rule', 'global', null, 'low', 0.9, null]
    ] as const;
    for (const [data, vaultId, type, scope, scopeKey, sensitivity, confidence, sourceTimestamp] of rows) {
      await pool.query(
        `INSERT INTO memories (
           vault_id, data, subject, hash, type, scope, scope_key, sensitivity, confidence, status, source_timestamp
         ) VALUES ($1, $2, 'applicability', $3, $4, $5, $6, $7, $8, 'active', $9)`,
        [vaultId, data, crypto.randomUUID(), type, scope, scopeKey, sensitivity, confidence, sourceTimestamp]
      );
    }

    const select = async (includeGlobalRules: boolean) => pool.query<{ data: string }>(
      `SELECT m.data FROM memories m
       WHERE m.vault_id = $1
         AND m.archived_at IS NULL
         AND m.status = 'active'
         AND ${memoryApplicabilityPredicateSql('m', '$2', '$3', '$4', '$5')}
         AND ${memoryEligibilityPredicateSql('m', '$6')}
       ORDER BY m.data`,
      [vaultA, null, 'project-a', null, includeGlobalRules, '2026-09-09T10:01:00Z']
    );

    expect((await select(false)).rows.map((row) => row.data)).toEqual(['global-fact', 'project-a']);
    expect((await select(true)).rows.map((row) => row.data)).toEqual(['global-fact', 'global-rule', 'project-a']);

    await expect(pool.query(
      `INSERT INTO memories (
         vault_id, data, subject, hash, type, scope, scope_key, sensitivity, confidence, status, source_timestamp
       ) VALUES ($1, 'restricted', 'applicability', $2, 'system_fact', 'project', 'project-a', 'restricted', 0.9, 'active', $3)`,
      [vaultA, crypto.randomUUID(), null]
    )).rejects.toThrow(/restricted memory cannot be activated/);
    await expect(pool.query(
      `INSERT INTO memories (
         vault_id, data, subject, hash, type, scope, scope_key, sensitivity, confidence, status, source_timestamp
       ) VALUES ($1, 'future', 'applicability', $2, 'system_fact', 'project', 'project-a', 'low', 0.9, 'active', $3)`,
      [vaultA, crypto.randomUUID(), new Date(Date.now() + 10 * 60_000).toISOString()]
    )).rejects.toThrow(/future source timestamp/);
  });
});
