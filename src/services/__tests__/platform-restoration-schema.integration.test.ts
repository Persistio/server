import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('restored platform schema (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  const otherVaultId = crypto.randomUUID();
  let closeDefaultPool = async () => {};
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    const db = await import('../../db/client');
    closeDefaultPool = db.closePool;
    await db.runMigrations();
    for (const id of [vaultId, otherVaultId]) {
      await pool.query('INSERT INTO vaults(id,name,api_key_hash) VALUES($1,$2,$3)', [id, `restoration-${id}`, crypto.randomUUID()]);
    }
  });
  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [[vaultId, otherVaultId]]);
    await pool.end();
    await closeDefaultPool();
  });

  async function insertMemory() {
    const result = await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,scope,scope_key,status)
      VALUES($1,'Example fact','Example',$2,'session','session-1','active') RETURNING id`, [vaultId, crypto.randomUUID()]);
    return result.rows[0].id as string;
  }

  it('has no live approval columns or recall delivery objects', async () => {
    const columns = await pool.query("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='memories'");
    expect(columns.rows.map(row => row.column_name).filter(name => /authority|approv|revok|decayed/.test(name))).toEqual([]);
    const tables = await pool.query("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'memory_delivery_%'");
    expect(tables.rows).toEqual([]);
    expect((await pool.query("SELECT proname FROM pg_proc WHERE proname LIKE 'persistio_%delivery%'")).rows).toEqual([]);
  });

  it('tracks substantive changes, not recall, and preserves mutation audit', async () => {
    const id = await insertMemory();
    await pool.query('UPDATE memories SET recall_count=recall_count+1,last_recalled=now(),revision=100 WHERE id=$1', [id]);
    expect((await pool.query('SELECT revision::text FROM memories WHERE id=$1', [id])).rows[0].revision).toBe('1');
    expect((await pool.query('SELECT id FROM memory_mutation_events WHERE memory_id=$1', [id])).rowCount).toBe(1);
    await pool.query("UPDATE memories SET data='Corrected fact' WHERE id=$1", [id]);
    expect((await pool.query('SELECT revision::text FROM memories WHERE id=$1', [id])).rows[0].revision).toBe('2');
    expect((await pool.query('SELECT revision::text FROM memory_contradiction_schedule WHERE memory_id=$1', [id])).rows).toEqual([{ revision: '2' }]);
    await expect(pool.query("UPDATE memory_mutation_events SET reason='tampered' WHERE memory_id=$1", [id])).rejects.toThrow('append-only');
    await pool.query('DELETE FROM memories WHERE id=$1', [id]);
    expect((await pool.query('SELECT id FROM memory_mutation_events WHERE memory_id=$1', [id])).rowCount).toBe(3);
  });

  it('has no candidate/review escape hatch for invalid scope or dates', async () => {
    const id = await insertMemory();
    for (const status of ['candidate', 'needs_review']) {
      await expect(pool.query('UPDATE memories SET status=$2 WHERE id=$1', [id, status])).rejects.toThrow();
    }
    await expect(pool.query('UPDATE memories SET scope_key=NULL WHERE id=$1', [id])).rejects.toThrow();
    await expect(pool.query("UPDATE memories SET valid_from='2026-12-01',valid_until='2026-01-01' WHERE id=$1", [id])).rejects.toThrow();
    await pool.query("UPDATE memories SET valid_from='2025-01-01',valid_until='2025-12-31' WHERE id=$1", [id]);
    expect((await pool.query('SELECT status FROM memories WHERE id=$1', [id])).rows[0].status).toBe('active');
  });

  it('allows manual improvement groups but rejects cross-vault target membership', async () => {
    const id = await insertMemory();
    const q = await pool.query('INSERT INTO curation_queue(vault_id,work_key) VALUES($1,$2) RETURNING id', [vaultId, crypto.randomUUID()]);
    const queueId = q.rows[0].id;
    await pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) VALUES($1,$2,$3,1)', [queueId, vaultId, id]);
    await expect(pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) VALUES($1,$2,$3,1)', [queueId, otherVaultId, id])).rejects.toThrow();
    await pool.query('DELETE FROM memories WHERE id=$1', [id]);
    expect((await pool.query('SELECT * FROM curation_queue_items WHERE queue_id=$1', [queueId])).rows).toEqual([]);
  });

  it('refuses populated-domain replacement without deleting data or vault configuration', async () => {
    const id = await insertMemory();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const migration = fs.readFileSync(path.resolve(__dirname, '../../db/migrations/057_restore_platform_pipeline.sql'), 'utf8');
      await expect(client.query(migration)).rejects.toThrow('approved empty memory domain');
      await client.query('ROLLBACK');
      expect((await client.query('SELECT id FROM memories WHERE id=$1', [id])).rowCount).toBe(1);
      expect((await client.query('SELECT id FROM vaults WHERE id=$1', [vaultId])).rowCount).toBe(1);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });
});
