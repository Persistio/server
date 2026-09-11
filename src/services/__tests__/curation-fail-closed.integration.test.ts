import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('curation fail-closed database boundary (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  const segmentId = crypto.randomUUID();

  beforeAll(async () => {
    const migration = await pool.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '054_inverted_validity_quarantine.sql') AS applied`
    );
    if (!migration.rows[0]?.applied) throw new Error('Test database must be migrated through 054_inverted_validity_quarantine.sql');
    await pool.query(
      `INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)`,
      [vaultId, `curation-policy-${vaultId}`, crypto.randomUUID()]
    );
    await pool.query(
      `INSERT INTO segments (id, vault_id, session_id, chunk_ids)
       VALUES ($1, $2, 'curation-policy', '{}'::uuid[])`,
      [segmentId, vaultId]
    );
  });

  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await pool.end();
  });

  it.each([
    ['policy rejection', 'low', 0.9, new Date().toISOString(), {
      policy_rejections: [{ code: 'untrusted_provenance', field: 'provenance', reason: 'imported' }]
    }, /policy-quarantined/],
    ['restricted sensitivity', 'restricted', 0.9, new Date().toISOString(), {}, /restricted/],
    ['invalid confidence', 'low', 0, new Date().toISOString(), {}, /invalid confidence/],
    ['future provenance', 'low', 0.9, new Date(Date.now() + 10 * 60_000).toISOString(), {}, /future source timestamp/]
  ])('rejects activation with %s', async (_label, sensitivity, confidence, sourceTimestamp, evidence, message) => {
    const inserted = await pool.query<{ id: string }>(
      `INSERT INTO memories (
         vault_id, data, subject, hash, scope, status, sensitivity, confidence,
         source_timestamp, evidence, source_segment_id
       ) VALUES ($1, 'candidate', 'policy', $2, 'global', 'candidate', $3, $4, $5, $6::jsonb, $7)
       RETURNING id`,
      [vaultId, crypto.randomUUID(), sensitivity, confidence, sourceTimestamp, JSON.stringify(evidence), segmentId]
    );

    await expect(pool.query(
      `UPDATE memories SET status = 'active' WHERE id = $1`,
      [inserted.rows[0].id]
    )).rejects.toThrow(message);
    expect((await pool.query<{ status: string }>(
      `SELECT status FROM memories WHERE id = $1`, [inserted.rows[0].id]
    )).rows[0].status).toBe('candidate');
  });

  it('rejects malformed policy metadata instead of treating it as clean', async () => {
    const inserted = await pool.query<{ id: string }>(
      `INSERT INTO memories (
         vault_id, data, subject, hash, scope, status, evidence, source_segment_id
       ) VALUES ($1, 'candidate', 'policy', $2, 'global', 'candidate',
                 '{"policy_rejections":"invalid"}'::jsonb, $3)
       RETURNING id`,
      [vaultId, crypto.randomUUID(), segmentId]
    );

    await expect(pool.query(
      `UPDATE memories SET status = 'active' WHERE id = $1`,
      [inserted.rows[0].id]
    )).rejects.toThrow(/policy-quarantined/);
  });

  it('rolls back the whole plan when a later action fails', async () => {
    const candidateIds = (await pool.query<{ id: string }>(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, status, source_segment_id)
       VALUES
         ($1, 'first', 'transaction', $2, 'global', 'candidate', $4),
         ($1, 'second', 'transaction', $3, 'global', 'candidate', $4)
       RETURNING id`,
      [vaultId, crypto.randomUUID(), crypto.randomUUID(), segmentId]
    )).rows.map((row) => row.id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`UPDATE memories SET status = 'active' WHERE id = $1`, [candidateIds[0]]);
      await expect(client.query(`UPDATE memories SET status = 'active' WHERE id = $1 AND false`, [candidateIds[1]]))
        .resolves.toMatchObject({ rowCount: 0 });
      throw new Error('injected action failure');
    } catch (error) {
      await client.query('ROLLBACK');
      expect(error).toMatchObject({ message: 'injected action failure' });
    } finally {
      client.release();
    }

    expect((await pool.query<{ status: string }>(
      `SELECT status FROM memories WHERE id = ANY($1::uuid[]) ORDER BY data`, [candidateIds]
    )).rows.map((row) => row.status)).toEqual(['candidate', 'candidate']);
  });

  it('does not reactivate an archived candidate', async () => {
    const archived = await pool.query<{ id: string }>(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, status, archived_at, source_segment_id)
       VALUES ($1, 'archived', 'policy', $2, 'global', 'candidate', now(), $3)
       RETURNING id`,
      [vaultId, crypto.randomUUID(), segmentId]
    );
    await expect(pool.query(
      `UPDATE memories SET status = 'active' WHERE id = $1`, [archived.rows[0].id]
    )).rejects.toThrow(/archived memory cannot be activated/);
    await expect(pool.query(
      `UPDATE memories SET status = 'active', archived_at = NULL WHERE id = $1`, [archived.rows[0].id]
    )).rejects.toThrow(/archived memory cannot be activated/);
  });

  it('rejects incoherent validity windows at the database boundary', async () => {
    await expect(pool.query(
      `INSERT INTO memories (
         vault_id, data, subject, hash, scope, status, valid_from, valid_until, source_segment_id
       ) VALUES ($1, 'invalid window', 'policy', $2, 'global', 'candidate',
                 '2026-07-01', '2026-06-30', $3)`,
      [vaultId, crypto.randomUUID(), segmentId]
    )).rejects.toMatchObject({ code: '23514' });
  });

  const rejection = { policy_rejections: [{ code: 'invalid_memory_validity_window', field: 'valid_until', reason: 'inverted' }] };
  const insertInverted = (status: string, evidence: unknown, archived = false) => pool.query<{ id: string }>(
    `INSERT INTO memories (vault_id, data, subject, hash, scope, status, evidence, valid_from, valid_until, archived_at)
     VALUES ($1, 'inverted', 'quarantine', $2, 'global', $3, $4::jsonb, '2026-07-01', '2026-06-30',
       CASE WHEN $5 THEN now() ELSE NULL END) RETURNING id`,
    [vaultId, crypto.randomUUID(), status, JSON.stringify(evidence), archived]
  );

  it.each([null, {}, [], { policy_rejections: null }, { policy_rejections: 'invalid' },
    { policy_rejections: [{ code: 'invalid_memory_validity_window', field: 'valid_until', reason: 'malformed' }] },
    { policy_rejections: { code: 'invalid_memory_validity_window', field: 'valid_until', reason: 'inverted' } }
  ])('rejects an inverted needs_review interval without exact structured evidence: %j', async evidence => {
    await expect(insertInverted('needs_review', evidence)).rejects.toMatchObject({ code: '23514' });
  });

  it.each(['candidate', 'active', 'superseded', 'contradicted'])('does not exempt unarchived %s rows', async status => {
    await expect(insertInverted(status, rejection)).rejects.toThrow();
  });

  it('preserves quarantine bounds through archival, refuses unsafe rollback and requires repair for eligibility', async () => {
    const id = (await insertInverted('needs_review', rejection)).rows[0].id;
    await expect(pool.query(`UPDATE memories SET evidence = '{}'::jsonb WHERE id = $1`, [id])).rejects.toMatchObject({ code: '23514' });
    await expect(pool.query(`UPDATE memories SET status = 'candidate' WHERE id = $1`, [id])).rejects.toMatchObject({ code: '23514' });
    await pool.query(`UPDATE memories SET status = 'superseded', archived_at = now() WHERE id = $1`, [id]);
    await expect(pool.query(`UPDATE memories SET archived_at = NULL WHERE id = $1`, [id])).rejects.toMatchObject({ code: '23514' });
    const row = (await pool.query(`SELECT valid_from::text, valid_until::text, evidence FROM memories WHERE id = $1`, [id])).rows[0];
    expect(row).toEqual({ valid_from: '2026-07-01', valid_until: '2026-06-30', evidence: rejection });
    const down = await fs.readFile(path.resolve('src/db/migrations/down/054_inverted_validity_quarantine.sql'), 'utf8');
    await expect(pool.query(down)).rejects.toThrow(/Cannot roll back temporal quarantine/);
    await pool.query(`UPDATE memories SET valid_until = '2026-07-02', status = 'needs_review', archived_at = NULL WHERE id = $1`, [id]);
    await pool.query(`UPDATE memories SET evidence = '{}'::jsonb, status = 'candidate' WHERE id = $1`, [id]);
    expect((await pool.query(`SELECT status FROM memories WHERE id = $1`, [id])).rows[0].status).toBe('candidate');
  });

  it('persists versioned validation metadata and before/after audit state', async () => {
    const run = await pool.query<{ id: string }>(
      `INSERT INTO curation_review_runs (
         vault_id, segment_id, model, schema_version, prompt_version, prompt_hash,
         validation_status, validation_errors, raw_response, before_state, after_state, applied_at
       ) VALUES ($1, $2, 'test-model', 'curation-plan.v1', 'curation-fail-closed.v1', $3,
                 'applied', '[]'::jsonb, '{"response":"encrypted-or-plain"}'::jsonb,
                 '[]'::jsonb, '[]'::jsonb, now())
       RETURNING id`,
      [vaultId, segmentId, 'a'.repeat(64)]
    );
    const stored = await pool.query<{
      schema_version: string;
      prompt_version: string;
      validation_status: string;
      has_before: boolean;
      has_after: boolean;
    }>(
      `SELECT schema_version, prompt_version, validation_status,
              before_state IS NOT NULL AS has_before,
              after_state IS NOT NULL AS has_after
       FROM curation_review_runs WHERE id = $1`,
      [run.rows[0].id]
    );
    expect(stored.rows[0]).toEqual({
      schema_version: 'curation-plan.v1',
      prompt_version: 'curation-fail-closed.v1',
      validation_status: 'applied',
      has_before: true,
      has_after: true
    });
  });
});
