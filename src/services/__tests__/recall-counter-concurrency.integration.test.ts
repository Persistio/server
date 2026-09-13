import crypto from 'node:crypto';
import { Pool, type PoolClient } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('best-effort recall counters under row contention (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  let counterSql: string;
  let closeDefaultPool = async () => {};
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    const db = await import('../../db/client');
    closeDefaultPool = db.closePool;
    await db.runMigrations();
    counterSql = (await import('../../routes/recall')).RECALL_COUNTER_UPDATE_SQL;
  });
  afterEach(async () => {
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]);
    vaults.length = 0;
  });
  afterAll(async () => { await pool.end(); await closeDefaultPool(); });

  async function fixture(count = 2) {
    const vaultId = crypto.randomUUID();
    await pool.query('INSERT INTO vaults(id,name,api_key_hash) VALUES($1,$2,$3)', [vaultId, 'recall-counter-lock-test', crypto.randomUUID()]);
    vaults.push(vaultId);
    const ids: string[] = [];
    for (let index = 0; index < count; index++) {
      const result = await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,scope,scope_key,status)
        VALUES($1,$2,'Counter regression',$3,'session','counter-session','active') RETURNING id`,
      [vaultId, `Useful fact ${index}`, crypto.randomUUID()]);
      ids.push(result.rows[0].id);
    }
    return { vaultId, ids };
  }

  async function transaction<T>(action: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Failure guard, not coordination: the other transaction remains locked
      // until the assertion completes, so an accidental blocking UPDATE fails.
      await client.query("SET LOCAL statement_timeout='2000ms'");
      const result = await action(client);
      await client.query('COMMIT');
      return result;
    } finally {
      await client.query('ROLLBACK');
      client.release();
    }
  }

  async function semanticState(vaultId: string) {
    return {
      memories: (await pool.query(`SELECT id,revision::text,data,subject,scope,scope_key,status,updated_at::text
        FROM memories WHERE vault_id=$1 ORDER BY id`, [vaultId])).rows,
      events: (await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1 ORDER BY id', [vaultId])).rows,
      schedule: (await pool.query(`SELECT memory_id,revision::text,generation,available_at::text,failures
        FROM memory_contradiction_schedule WHERE vault_id=$1 ORDER BY memory_id`, [vaultId])).rows,
      pending: (await pool.query(`SELECT pending_count::text,revision::text,next_visit_at::text
        FROM memory_contradiction_pending_vaults WHERE vault_id=$1`, [vaultId])).rows
    };
  }

  async function counters(vaultId: string) {
    return (await pool.query('SELECT id,recall_count,last_recalled FROM memories WHERE vault_id=$1 ORDER BY id', [vaultId])).rows;
  }

  it('skips a locked returned memory while updating an unlocked one without semantic side effects', async () => {
    const { vaultId, ids } = await fixture();
    const before = await semanticState(vaultId);
    await transaction(async holder => {
      await holder.query('SELECT id FROM memories WHERE id=$1 FOR UPDATE', [ids[0]]);
      const updated = await transaction(client => client.query(counterSql, [vaultId, ids]));
      expect(updated.rowCount).toBe(1);
      const rows = await counters(vaultId);
      expect(rows.find(row => row.id === ids[0])).toMatchObject({ recall_count: 0, last_recalled: null });
      expect(rows.find(row => row.id === ids[1])).toMatchObject({ recall_count: 1, last_recalled: expect.any(Date) });
      expect(await semanticState(vaultId)).toEqual(before);
    });
  });

  it('completes counters and actual vault deletion while the deleting transaction holds parent and child locks', async () => {
    const { vaultId, ids } = await fixture();
    await transaction(async deleting => {
      await deleting.query('SELECT id FROM vaults WHERE id=$1 FOR UPDATE', [vaultId]);
      await deleting.query('SELECT id FROM memories WHERE id=$1 FOR UPDATE', [ids[0]]);
      // This models the observed partial cascade lock order without relying on
      // the planner choosing a particular physical deletion order or any sleep.
      const updated = await transaction(client => client.query(counterSql, [vaultId, ids]));
      expect(updated.rowCount).toBe(1);
      const removed = await deleting.query('DELETE FROM vaults WHERE id=$1 RETURNING id', [vaultId]);
      expect(removed.rows).toEqual([{ id: vaultId }]);
    });
    expect((await pool.query('SELECT id FROM vaults WHERE id=$1', [vaultId])).rows).toEqual([]);
    expect(await counters(vaultId)).toEqual([]);
    expect((await pool.query('SELECT memory_id FROM memory_contradiction_schedule WHERE vault_id=$1', [vaultId])).rows).toEqual([]);
  });

  it('increments duplicate requested IDs once, excludes other vaults, and safely ignores missing IDs', async () => {
    const current = await fixture(1), other = await fixture(1);
    const before = await semanticState(current.vaultId), otherBefore = await semanticState(other.vaultId);
    const updated = await transaction(client => client.query(counterSql, [current.vaultId,
      [current.ids[0], current.ids[0], other.ids[0], crypto.randomUUID()]]));
    expect(updated.rowCount).toBe(1);
    expect(await counters(current.vaultId)).toEqual([{ id: current.ids[0], recall_count: 1, last_recalled: expect.any(Date) }]);
    expect(await counters(other.vaultId)).toEqual([{ id: other.ids[0], recall_count: 0, last_recalled: null }]);
    expect(await semanticState(current.vaultId)).toEqual(before);
    expect(await semanticState(other.vaultId)).toEqual(otherBefore);
    expect((await transaction(client => client.query(counterSql, [current.vaultId, []]))).rowCount).toBe(0);
  });
});
