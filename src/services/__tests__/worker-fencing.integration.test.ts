import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('worker fencing and vault integrity (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultA = crypto.randomUUID();
  const vaultB = crypto.randomUUID();
  const chunkA = crypto.randomUUID();
  const chunkB = crypto.randomUUID();
  const segmentA = crypto.randomUUID();
  const receiptQueueId = crypto.randomUUID();

  beforeAll(async () => {
    const migration = await pool.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '051_worker_fencing_and_vault_integrity.sql') AS applied`
    );
    if (!migration.rows[0]?.applied) throw new Error('Test database must be migrated through 051_worker_fencing_and_vault_integrity.sql');
    await pool.query(
      `INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3), ($4, $5, $6)`,
      [vaultA, `fencing-a-${vaultA}`, crypto.randomUUID(), vaultB, `fencing-b-${vaultB}`, crypto.randomUUID()]
    );
    await pool.query(
      `INSERT INTO raw_chunks (id, vault_id, session_id, role) VALUES
       ($1, $2, 'lease-a', 'user'), ($3, $4, 'lease-b', 'user')`,
      [chunkA, vaultA, chunkB, vaultB]
    );
    await pool.query(
      `INSERT INTO segments (id, vault_id, session_id, chunk_ids) VALUES ($1, $2, 'lease-a', $3::uuid[])`,
      [segmentA, vaultA, [chunkA]]
    );
  });

  afterAll(async () => {
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id = $1', [receiptQueueId]);
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [[vaultA, vaultB]]);
    await pool.end();
  });

  it('permits takeover only after expiry and rejects the stale transition token', async () => {
    const queueId = crypto.randomUUID();
    const oldToken = crypto.randomUUID();
    const newToken = crypto.randomUUID();
    await pool.query(
      `INSERT INTO extraction_queue (
         id, vault_id, segment_id, claimed_at, claimed_by, claim_token, lease_expires_at
       ) VALUES ($1, $2, $3, now() - interval '2 minutes', 'old', $4, now() - interval '1 minute')`,
      [queueId, vaultA, segmentA, oldToken]
    );
    const takeover = await pool.query(
      `UPDATE extraction_queue
       SET claimed_at = now(), claimed_by = 'new', claim_token = $2, lease_expires_at = now() + interval '10 minutes'
       WHERE id = $1 AND lease_expires_at <= now()`,
      [queueId, newToken]
    );
    expect(takeover.rowCount).toBe(1);
    expect((await pool.query(
      `DELETE FROM extraction_queue WHERE id = $1 AND claim_token = $2`, [queueId, oldToken]
    )).rowCount).toBe(0);
    expect((await pool.query(
      `DELETE FROM extraction_queue WHERE id = $1 AND claim_token = $2`, [queueId, newToken]
    )).rowCount).toBe(1);
  });

  it('rejects cross-vault segment chunks and queue associations', async () => {
    await expect(pool.query(
      `INSERT INTO segments (vault_id, session_id, chunk_ids) VALUES ($1, 'cross-vault', $2::uuid[])`,
      [vaultA, [chunkB]]
    )).rejects.toMatchObject({ code: '23503' });
    await expect(pool.query(
      `INSERT INTO extraction_queue (vault_id, segment_id) VALUES ($1, $2)`,
      [vaultB, segmentA]
    )).rejects.toMatchObject({ code: '23503' });
    await expect(pool.query(
      `INSERT INTO curation_queue (vault_id, segment_id, work_key) VALUES ($1, $2, gen_random_uuid()::text)`,
      [vaultB, segmentA]
    )).rejects.toMatchObject({ code: '23503' });
  });

  it('rejects cross-vault evidence, parents, and graph edges', async () => {
    const memoryA = (await pool.query<{ id: string }>(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, status, source_chunks, source_segment_id)
       VALUES ($1, 'a', 'a', $2, 'global', 'active', $3::uuid[], $4) RETURNING id`,
      [vaultA, crypto.randomUUID(), [chunkA], segmentA]
    )).rows[0].id;
    const memoryB = (await pool.query<{ id: string }>(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, status, source_chunks)
       VALUES ($1, 'b', 'b', $2, 'global', 'active', $3::uuid[]) RETURNING id`,
      [vaultB, crypto.randomUUID(), [chunkB]]
    )).rows[0].id;

    await expect(pool.query(
      `UPDATE memories SET source_chunks = $2::uuid[] WHERE id = $1`, [memoryA, [chunkB]]
    )).rejects.toMatchObject({ code: '23503' });
    await expect(pool.query(
      `UPDATE memories SET parent_id = $2 WHERE id = $1`, [memoryA, memoryB]
    )).rejects.toMatchObject({ code: '23503' });
    await expect(pool.query(
      `INSERT INTO memory_edges (vault_id, from_memory_id, to_memory_id, type)
       VALUES ($1, $2, $3, 'supports')`, [vaultA, memoryA, memoryB]
    )).rejects.toMatchObject({ code: '23503' });
  });

  it('rolls action receipts back with failed side effects and suppresses committed replays', async () => {
    const rolledBackMemoryId = crypto.randomUUID();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO worker_action_receipts (queue_kind, queue_id, action_key, claim_token)
         VALUES ('extraction', $1, 'extract-and-complete', $2)`,
        [receiptQueueId, crypto.randomUUID()]
      );
      await client.query(
        `INSERT INTO memories (id, vault_id, data, subject, hash, scope, status)
         VALUES ($1, $2, 'rolled back', 'fencing', $3, 'global', 'active')`,
        [rolledBackMemoryId, vaultA, crypto.randomUUID()]
      );
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }
    expect((await pool.query(
      `SELECT 1 FROM worker_action_receipts WHERE queue_id = $1`, [receiptQueueId]
    )).rowCount).toBe(0);
    expect((await pool.query(`SELECT 1 FROM memories WHERE id = $1`, [rolledBackMemoryId])).rowCount).toBe(0);

    const first = await pool.query(
      `INSERT INTO worker_action_receipts (queue_kind, queue_id, action_key, claim_token)
       VALUES ('extraction', $1, 'extract-and-complete', $2)
       ON CONFLICT (queue_kind, queue_id, action_key) DO NOTHING
       RETURNING queue_id`,
      [receiptQueueId, crypto.randomUUID()]
    );
    const replay = await pool.query(
      `INSERT INTO worker_action_receipts (queue_kind, queue_id, action_key, claim_token)
       VALUES ('extraction', $1, 'extract-and-complete', $2)
       ON CONFLICT (queue_kind, queue_id, action_key) DO NOTHING
       RETURNING queue_id`,
      [receiptQueueId, crypto.randomUUID()]
    );
    expect(first.rowCount).toBe(1);
    expect(replay.rowCount).toBe(0);
  });
});
