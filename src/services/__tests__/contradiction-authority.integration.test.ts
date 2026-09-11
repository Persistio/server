import crypto from 'node:crypto';

import { Pool, type PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { GLOBAL_RULE_POLICIES, isMemoryRecallable, memoryAuthorityPredicateSql } from '../memory-authority';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
type Event = { type: 'approve' | 'revoke' | 'migration'; version: number; snapshot?: unknown; source?: string; actor?: string; newState?: string };
const legacySnapshot = { type: 'user_rule', scope: 'global', status: 'active', archived_at: null };
const variants = [
  { name: 'global rule', type: 'user_rule', scope: 'global', required: true },
  { name: 'scoped rule', type: 'user_rule', scope: 'project', required: true },
  { name: 'legacy global fact', type: 'system_fact', scope: 'global', required: false },
  { name: 'controlled scoped fact', type: 'system_fact', scope: 'project', required: true },
  { name: 'nullable legacy fact', type: null, scope: 'project', required: false }
] as const;
const histories: Array<{ name: string; state: string; version: number; events: Event[];
  approval?: boolean; revocation?: boolean; legacy?: boolean }> = [
  { name: 'unapproved', state: 'proposed', version: 3, events: [] },
  { name: 'approved with current proof', state: 'approved', version: 3,
    events: [{ type: 'approve', version: 3 }], approval: true },
  { name: 'approved without proof', state: 'approved', version: 3, events: [] },
  { name: 'approved with malformed proof state', state: 'approved', version: 3,
    events: [{ type: 'approve', version: 3, newState: 'proposed' }] },
  { name: 'approved with stale proof', state: 'approved', version: 3, events: [{ type: 'approve', version: 2 }] },
  { name: 'exact legacy migration', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: legacySnapshot }], legacy: true },
  { name: 'stale legacy migration', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 2, snapshot: legacySnapshot }] },
  { name: 'legacy missing snapshot', state: 'proposed', version: 3, events: [{ type: 'migration', version: 3 }] },
  { name: 'legacy wrong lifecycle', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: { ...legacySnapshot, status: 'candidate' } }] },
  { name: 'legacy archived snapshot', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: { ...legacySnapshot, archived_at: '2026-01-01' } }] },
  { name: 'legacy non-system actor', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: legacySnapshot, actor: 'user' }] },
  { name: 'legacy non-migration source', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: legacySnapshot, source: 'api' }] },
  { name: 'revoked', state: 'revoked', version: 3,
    events: [{ type: 'approve', version: 2 }, { type: 'revoke', version: 3 }], revocation: true },
  { name: 'approved with sticky revocation', state: 'approved', version: 3,
    events: [{ type: 'approve', version: 3 }, { type: 'revoke', version: 4 }], approval: true, revocation: true },
  { name: 'legacy with sticky revocation', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: legacySnapshot }, { type: 'revoke', version: 2 }],
    legacy: true, revocation: true },
  { name: 'future approval cannot clear current revocation', state: 'approved', version: 3,
    events: [{ type: 'approve', version: 3 }, { type: 'revoke', version: 4 }, { type: 'approve', version: 5 }],
    approval: true, revocation: true },
  { name: 'future approval cannot restore revoked legacy authority', state: 'proposed', version: 3,
    events: [{ type: 'migration', version: 3, snapshot: legacySnapshot }, { type: 'revoke', version: 2 },
      { type: 'approve', version: 4 }], legacy: true, revocation: true },
  { name: 'malformed later approval cannot restore revoked legacy authority', state: 'proposed', version: 5,
    events: [{ type: 'migration', version: 5, snapshot: legacySnapshot }, { type: 'revoke', version: 2 },
      { type: 'approve', version: 3, newState: 'proposed' }], legacy: true, revocation: true },
  { name: 'reapproved after revocation', state: 'approved', version: 3,
    events: [{ type: 'approve', version: 1 }, { type: 'revoke', version: 2 }, { type: 'approve', version: 3 }], approval: true }
];

describe.skipIf(!databaseUrl)('contradiction authority projection (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];

  beforeAll(async () => {
    // Schema is installed by the surrounding integration run. Never mutate a
    // concurrently used schema from this authority/concurrency regression suite.
    await pool.query('SELECT contradiction_authority_eligible(NULL::memories, $1)', ['off']);
  });
  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [vaults]);
    await pool.end();
  });

  async function addMemory(options: { type?: string | null; scope?: string } = {}) {
    const vaultId = crypto.randomUUID();
    const id = crypto.randomUUID();
    vaults.push(vaultId);
    await pool.query('INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)',
      [vaultId, `authority-projection-${vaultId}`, crypto.randomUUID()]);
    await pool.query(
      `INSERT INTO memories (id, vault_id, data, subject, hash, type, scope, scope_key)
       VALUES ($1, $2, 'authority projection fact', 'authority', $3, $4, $5, $6)`,
      [id, vaultId, crypto.randomUUID(), options.type === undefined ? 'user_rule' : options.type,
        options.scope ?? 'global', (options.scope ?? 'global') === 'global' ? null : 'authority-project']
    );
    return { id, vaultId };
  }

  async function insertEvent(client: Pick<PoolClient, 'query'>, memory: { id: string; vaultId: string }, event: Event) {
    await client.query(
      `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
         new_version, actor_type, source, reason, snapshot)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'authority lifecycle regression', $8::jsonb)`,
      [memory.vaultId, memory.id, event.type,
        event.newState ?? (event.type === 'approve' ? 'approved' : event.type === 'revoke' ? 'revoked' : 'proposed'),
        event.version, event.actor ?? (event.type === 'migration' ? 'system' : 'user'),
        event.source ?? (event.type === 'migration' ? 'migration' : 'api'),
        event.snapshot === undefined ? null : JSON.stringify(event.snapshot)]
    );
  }

  async function schedule(id: string) {
    return (await pool.query<{ policy: string; authority_ready: boolean; generation: string }>(
      `SELECT policy, authority_ready, generation::text
       FROM memory_contradiction_schedule WHERE memory_id = $1 ORDER BY policy`, [id]
    )).rows;
  }

  async function expectReady(id: string, approved: boolean, legacy = approved) {
    expect((await schedule(id)).map(({ policy, authority_ready }) => ({ policy, authority_ready }))).toEqual([
      { policy: 'approved_only', authority_ready: approved },
      { policy: 'legacy', authority_ready: legacy },
      { policy: 'off', authority_ready: false }
    ]);
  }

  it.each(variants.flatMap(variant => histories.map(history => ({ variant, history,
    label: `${variant.name}: ${history.name}` }))))('matches SQL, JS and durable policy projection for $label', async ({ variant, history }) => {
    const memory = await addMemory(variant);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('UPDATE memories SET authority_state = $2, authority_version = $3 WHERE id = $1',
        [memory.id, history.state, history.version]);
      for (const event of history.events) await insertEvent(client, memory, event);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally { client.release(); }

    for (const policy of GLOBAL_RULE_POLICIES) {
      // Current inserts always require authority. Override only this composite
      // query value to represent grandfathered factual rows without bypassing
      // database write guards or altering triggers in a shared test database.
      const projected = await pool.query<{ runtime: boolean; projection: boolean }>(
        `WITH m AS (
           SELECT (jsonb_populate_record(memories, jsonb_build_object('authority_required', $2::boolean))).*
           FROM memories WHERE id = $1
         ) SELECT ${memoryAuthorityPredicateSql('m', '$3')} AS runtime,
           contradiction_authority_eligible(m::memories, $3) AS projection FROM m`,
        [memory.id, variant.required, policy]
      );
      const expected = isMemoryRecallable(variant.type, variant.scope, history.state,
        history.approval ?? false, policy, variant.required, history.revocation ?? false, history.legacy ?? false);
      expect(projected.rows[0], `${variant.name}/${history.name}/${policy}`).toEqual({ runtime: expected, projection: expected });
      if (variant.required) {
        const row = (await schedule(memory.id)).find(row => row.policy === policy);
        expect(row?.authority_ready, `stored readiness for ${policy}`).toBe(expected);
      }
    }
  });

  it('reconciles approval proof inserted after a memory update in the API CTE', async () => {
    const memory = await addMemory();
    const before = await schedule(memory.id);
    await pool.query(
      `WITH target AS (
         SELECT id, authority_version FROM memories WHERE id = $1 FOR UPDATE
       ), updated AS (
         UPDATE memories SET authority_state = 'approved', authority_version = target.authority_version + 1
         FROM target WHERE memories.id = target.id RETURNING memories.id, memories.vault_id, memories.authority_version
       ), authority_audit AS (
         INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state, new_version, actor_type, source, reason)
         SELECT vault_id, id, 'approve', 'approved', authority_version, 'user', 'api', 'API ordering regression' FROM updated
         RETURNING id
       ) SELECT updated.id, (SELECT id FROM authority_audit) FROM updated`, [memory.id]
    );
    await expectReady(memory.id, true);
    for (const old of before) {
      expect((await pool.query(
        'DELETE FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = $2 AND generation = $3',
        [memory.id, old.policy, old.generation]
      )).rowCount).toBe(0);
    }
  });

  it('reconciles event-before-state ordering using the final committed row', async () => {
    const memory = await addMemory();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await insertEvent(client, memory, { type: 'approve', version: 2 });
      await client.query("UPDATE memories SET authority_state = 'approved', authority_version = 2 WHERE id = $1", [memory.id]);
      await expectReady(memory.id, false);
      await client.query('COMMIT');
      await expectReady(memory.id, true);
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  });

  it('rolls back memory state, event proof and durable readiness atomically', async () => {
    const memory = await addMemory();
    const before = await schedule(memory.id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("UPDATE memories SET authority_state = 'approved', authority_version = 2 WHERE id = $1", [memory.id]);
      await insertEvent(client, memory, { type: 'approve', version: 2 });
      await client.query('SET CONSTRAINTS ALL IMMEDIATE');
      await client.query('ROLLBACK');
    } finally { client.release(); }
    expect(await schedule(memory.id)).toEqual(before);
    expect((await pool.query('SELECT authority_state, authority_version FROM memories WHERE id = $1', [memory.id])).rows[0])
      .toEqual({ authority_state: 'proposed', authority_version: 1 });
    expect((await pool.query('SELECT 1 FROM memory_authority_events WHERE memory_id = $1', [memory.id])).rowCount).toBe(0);
  });

  it('serializes two event-only commits without deadlock or stale final readiness', async () => {
    const memory = await addMemory();
    await pool.query("UPDATE memories SET authority_state = 'approved' WHERE id = $1", [memory.id]);
    const first = await pool.connect();
    const second = await pool.connect();
    try {
      for (const client of [first, second]) {
        await client.query('BEGIN');
        await client.query("SET LOCAL lock_timeout = '3s'");
      }
      await Promise.all([
        insertEvent(first, memory, { type: 'approve', version: 1 }),
        insertEvent(second, memory, { type: 'revoke', version: 1 })
      ]);
      await Promise.all([first.query('COMMIT'), second.query('COMMIT')]);
      await expectReady(memory.id, false);
      expect((await pool.query('SELECT count(*)::int AS count FROM memory_authority_events WHERE memory_id = $1', [memory.id])).rows[0].count).toBe(2);
    } finally {
      await Promise.all([first.query('ROLLBACK'), second.query('ROLLBACK')]);
      first.release(); second.release();
    }
  });

  it('fences each authority-event refresh without granting authority to historical proof', async () => {
    const memory = await addMemory();
    await pool.query("UPDATE memories SET authority_state = 'approved', authority_version = 3 WHERE id = $1", [memory.id]);
    const missingProof = await schedule(memory.id);
    await insertEvent(pool, memory, { type: 'approve', version: 3 });
    const approved = await schedule(memory.id);
    await expectReady(memory.id, true);
    for (const policy of ['approved_only', 'legacy']) {
      expect(approved.find(row => row.policy === policy)?.generation)
        .not.toBe(missingProof.find(row => row.policy === policy)?.generation);
    }
    await insertEvent(pool, memory, { type: 'approve', version: 1 });
    const historical = await schedule(memory.id);
    expect(historical.map(({ policy, authority_ready }) => ({ policy, authority_ready })))
      .toEqual(approved.map(({ policy, authority_ready }) => ({ policy, authority_ready })));
    for (const row of historical) {
      expect(row.generation).not.toBe(approved.find(previous => previous.policy === row.policy)?.generation);
    }
    await insertEvent(pool, memory, { type: 'revoke', version: 4 });
    await expectReady(memory.id, false);
    for (const old of historical) {
      expect((await pool.query(
        'DELETE FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = $2 AND generation = $3',
        [memory.id, old.policy, old.generation]
      )).rowCount).toBe(0);
    }
  });
});
