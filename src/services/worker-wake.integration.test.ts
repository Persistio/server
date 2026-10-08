import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { Pool, type PoolClient } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { WorkerWakeStore, requestWorkerWakes, completeWorkerWake, admitContradictionArbitration } from './worker-wake-store';
import { WorkerWakeDispatcher } from './worker-wake-dispatcher';
import { WorkerWakeReconciliation } from './worker-wake-reconciliation';
import { WORKER_WAKE_KINDS, type WorkerWakeKind } from './worker-wake-types';

const url = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('durable wake protocol (PostgreSQL 17)', () => {
  const pool = new Pool({ connectionString: url });
  const store = new WorkerWakeStore(pool);
  const due = new Date('2020-01-01Z');
  const vault = crypto.randomUUID();
  const raw = crypto.randomUUID();
  const event = crypto.randomUUID();
  const transport = { create: vi.fn(), exists: vi.fn() };
  const reconciliation = { dueAt: vi.fn() };
  const dispatch = () => new WorkerWakeDispatcher({
    enabled: true, store, transport, reconciliation,
    contradictionAllowance: 2, contradictionCycleMs: 30_000
  }).dispatchOnce();
  const row = async (kind: WorkerWakeKind = 'extraction') =>
    (await pool.query('SELECT * FROM worker_wake_intents WHERE kind=$1', [kind])).rows[0];
  async function request(kind: WorkerWakeKind = 'extraction', at = due, client?: PoolClient) {
    const owned = client ?? await pool.connect();
    try {
      if (!client) await owned.query('BEGIN');
      await requestWorkerWakes(owned, [{ kind, dueAt: at }], true);
      if (!client) await owned.query('COMMIT');
    } finally { if (!client) owned.release(); }
  }
  async function ready(kind: WorkerWakeKind = 'extraction') {
    await pool.query("UPDATE worker_wake_intents SET next_attempt_at=now()-interval '1 second', dispatch_expires_at=CASE WHEN dispatch_owner IS NOT NULL THEN now()-interval '1 second' END WHERE kind=$1", [kind]);
  }

  beforeAll(async () => {
    if (!url || process.env.DATABASE_URL !== url || !/^persistio_test_/.test(new URL(url).pathname.slice(1))) {
      throw new Error('Use matching isolated persistio_test_ database URLs');
    }
    const db = await import('../db/client');
    try { await db.runMigrations(); } finally { await db.closePool(); }
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'wake-fixture',$2,'unlimited')", [vault, crypto.randomUUID()]);
  });
  beforeEach(async () => {
    vi.resetAllMocks();
    transport.create.mockResolvedValue('created');
    transport.exists.mockResolvedValue(true);
    reconciliation.dueAt.mockResolvedValue(null);
    await pool.query('DELETE FROM worker_wake_intents');
    await pool.query("UPDATE vaults SET rate_limit_override=NULL WHERE id=$1", [vault]);
    await pool.query('DELETE FROM extraction_queue WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM curation_queue WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM raw_chunks WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM vault_usage WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM vault_curation_state WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM memory_contradiction_pending_vaults WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM raw_chunk_blob_write_intents WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM platform_event_outbox WHERE event_id=$1', [event]);
  });
  afterAll(async () => {
    await pool.query('DELETE FROM worker_wake_intents');
    await pool.query('DELETE FROM platform_event_outbox WHERE event_id=$1', [event]);
    await pool.query('DELETE FROM raw_chunk_blob_write_intents WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM vaults WHERE id=$1', [vault]);
    await pool.end();
  });

  it('disabled capture/dispatch performs no database or transport work', async () => {
    const query = vi.fn();
    await requestWorkerWakes({ query } as unknown as PoolClient, [{ kind: 'extraction', dueAt: due }]);
    const result = await new WorkerWakeDispatcher({
      store: new WorkerWakeStore({ query } as unknown as Pool), transport, reconciliation,
      contradictionAllowance: 2, contradictionCycleMs: 30_000
    }).dispatchOnce();
    expect(result).toEqual({ enabled: false, outcomes: {}, failures: {} });
    expect(query).not.toHaveBeenCalled();
    expect(reconciliation.dueAt).not.toHaveBeenCalled();
    expect(transport.create).not.toHaveBeenCalled();
  });

  it('rolls back durable work and its intent together; coalesces requests per kind', async () => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("INSERT INTO raw_chunks(id,vault_id,session_id,role) VALUES($1,$2,'wake','user')", [raw, vault]);
      await request('extraction', due, client);
      await client.query('ROLLBACK');
      expect(await row()).toBeUndefined();
      expect((await pool.query('SELECT 1 FROM raw_chunks WHERE id=$1', [raw])).rowCount).toBe(0);
      await client.query('BEGIN');
      await requestWorkerWakes(client, [
        { kind: 'curation', dueAt: due }, { kind: 'extraction', dueAt: new Date('2030-01-01Z') },
        { kind: 'extraction', dueAt: due }
      ], true);
      await client.query('COMMIT');
      expect((await row()).requested_generation).toBe('1');
      expect((await row()).pending_due_at).toEqual(due);
      expect((await pool.query('SELECT * FROM worker_wake_intents')).rowCount).toBe(2);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  it('retries the same token after create succeeded but its result/SQL mark was lost', async () => {
    await request();
    const first = await store.reserve('extraction', 2, 30_000);
    expect(first).not.toBeNull();
    // The remote side accepts; this dispatcher dies before recording success.
    await transport.create(first!.wake, first!.dueAt);
    await ready();
    transport.create.mockResolvedValue('exists');
    const report = await dispatch();
    expect(report.outcomes.extraction).toBe('retained');
    expect(transport.create.mock.calls[1][0]).toEqual(first!.wake);
    expect((await row()).completed_generation).toBe('0');
    expect((await row()).dispatched).toBe(true);
  });

  it('retains ambiguous creates, fences expired owners and permits just one concurrent reservation', async () => {
    await request();
    const claims = await Promise.all(Array.from({ length: 8 }, () => store.reserve('extraction', 2, 30_000)));
    const first = claims.find(Boolean)!;
    expect(claims.filter(Boolean)).toHaveLength(1);
    await ready();
    const next = (await store.reserve('extraction', 2, 30_000))!;
    expect(next.wake).toEqual(first.wake);
    expect(next.owner).not.toBe(first.owner);
    expect(await store.dispatched(first)).toBe(false);
    expect(await store.rotate(first)).toBe(false);
    expect(await store.failed(next)).toBe(true);
    expect((await row()).active_token).toBe(first.wake.token);
    expect((await row()).attempts).toBe(1);
    expect(await store.reserve('extraction', 2, 30_000)).toBeNull();
  });

  it('preserves a concurrent producer committed after reservation and before completion', async () => {
    await request();
    const reserved = (await store.reserve('extraction', 2, 30_000))!;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await request('extraction', new Date('2030-01-01Z'), client);
      let completed = false;
      const completing = completeWorkerWake(pool, reserved.wake).then(result => { completed = true; return result; });
      // Ensure a real second connection is waiting on the producer's row lock.
      await vi.waitFor(async () => {
        const waiting = await pool.query("SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE worker_wake_intents SET completed_generation%'");
        expect(waiting.rowCount).toBeGreaterThan(0);
      });
      expect(completed).toBe(false);
      await client.query('COMMIT');
      expect(await completing).toBe(true);
      expect(await row()).toMatchObject({ requested_generation: '2', completed_generation: '1', active_token: null });
      expect((await row()).pending_due_at).toEqual(new Date('2030-01-01Z'));
      expect(await completeWorkerWake(pool, reserved.wake)).toBe(false);
      expect(await store.dispatched(reserved)).toBe(false);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  it('handles early completion before create acknowledgement and out-of-order old deliveries', async () => {
    await request();
    const old = (await store.reserve('extraction', 2, 30_000))!;
    expect(await completeWorkerWake(pool, old.wake)).toBe(true);
    expect(await store.dispatched(old)).toBe(false);
    await request();
    const current = (await store.reserve('extraction', 2, 30_000))!;
    expect(await completeWorkerWake(pool, old.wake)).toBe(false);
    expect((await row()).active_token).toBe(current.wake.token);
    expect(await completeWorkerWake(pool, current.wake)).toBe(true);
  });

  it('rotates confirmed deleted/tombstoned task names without acknowledging or deleting business work', async () => {
    await request();
    await dispatch();
    const before = await row();
    await ready();
    transport.exists.mockResolvedValue(false);
    expect((await dispatch()).outcomes.extraction).toBe('rotated');
    const after = await row();
    expect(after.active_token).not.toBe(before.active_token);
    expect(after.active_generation).toBe(before.active_generation);
    expect(after.completed_generation).toBe('0');
    transport.create.mockResolvedValue('exists');
    expect((await dispatch()).outcomes.extraction).toBe('rotated');
    expect((await row()).active_token).not.toBe(after.active_token);
  });

  it('retains paused tasks; reports errors per kind and continues other kinds', async () => {
    await request();
    await request('curation');
    transport.create.mockRejectedValueOnce(new Error('queue unavailable'));
    const report = await dispatch();
    expect(report.outcomes.extraction).toBe('failed');
    expect(report.failures.extraction).toBe('transport');
    expect(report.outcomes.curation).toBe('created');
    await ready('curation');
    expect((await dispatch()).outcomes.curation).toBe('retained');
    expect((await row('curation')).completed_generation).toBe('0');
    expect((await row()).attempts).toBe(1);
  });

  it('bounds a blocked kind at the database and still dispatches another kind', async () => {
    await request();
    await request('curation');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SELECT 1 FROM worker_wake_intents WHERE kind='extraction' FOR UPDATE");
      const report = await dispatch();
      expect(report.outcomes.extraction).toBe('failed');
      expect(report.failures.extraction).toBe('storage');
      expect(report.outcomes.curation).toBe('created');
      expect((await row()).active_token).toBeNull();
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  it('exposes reconciliation failure separately and does not falsely acknowledge work', async () => {
    await request();
    reconciliation.dueAt.mockRejectedValueOnce(new Error('scan unavailable'));
    const report = await dispatch();
    expect(report.outcomes.extraction).toBe('failed');
    expect(report.failures.extraction).toBe('reconciliation');
    expect((await row()).completed_generation).toBe('0');
    expect(transport.create).not.toHaveBeenCalled();
  });

  it('bounds failure state/backoff without growing rows', async () => {
    await request();
    for (let i = 0; i < 24; i++) {
      await ready();
      const reservation = (await store.reserve('extraction', 2, 30_000))!;
      await store.failed(reservation);
    }
    expect((await row()).attempts).toBe(20);
    expect((await row()).next_attempt_at.getTime() - Date.now()).toBeLessThanOrEqual(301_000);
    expect((await pool.query('SELECT * FROM worker_wake_intents')).rowCount).toBe(1);
  });

  it('shares a durable contradiction allowance across duplicates and transport replacements', async () => {
    await request('contradiction');
    const first = (await store.reserve('contradiction', 2, 30_000))!;
    const admissions = await Promise.all(Array.from({ length: 8 }, () => admitContradictionArbitration(pool, first.wake)));
    expect(admissions.filter(Boolean)).toHaveLength(2);
    await store.rotate(first);
    const retry = (await store.reserve('contradiction', 999, 30_000))!;
    expect(retry.wake.generation).toBe(first.wake.generation);
    expect(await admitContradictionArbitration(pool, retry.wake)).toBe(false);
    expect(await admitContradictionArbitration(pool, first.wake)).toBe(false);
    await request('contradiction');
    // Simulate a task that sat paused beyond its initial cycle deadline.
    await pool.query("UPDATE worker_wake_intents SET next_cycle_at=now()-interval '1 day' WHERE kind='contradiction'");
    await completeWorkerWake(pool, retry.wake);
    expect((await row('contradiction')).next_cycle_at.getTime()).toBeGreaterThan(Date.now() + 29_000);
    expect(await store.reserve('contradiction', 2, 30_000)).toBeNull();
    await pool.query("UPDATE worker_wake_intents SET next_cycle_at=now()-interval '1 second' WHERE kind='contradiction'");
    const next = (await store.reserve('contradiction', 2, 30_000))!;
    expect(next.wake.generation).not.toBe(retry.wake.generation);
    expect(await admitContradictionArbitration(pool, next.wake)).toBe(true);
  });

  it('preserves bigint generations above the JavaScript safe integer limit', async () => {
    await request();
    await pool.query("UPDATE worker_wake_intents SET requested_generation=9007199254740993");
    const reserved = (await store.reserve('extraction', 2, 30_000))!;
    expect(reserved.wake.generation).toBe('9007199254740993');
    await request();
    await completeWorkerWake(pool, reserved.wake);
    expect(await row()).toMatchObject({ requested_generation: '9007199254740994', completed_generation: '9007199254740993' });
  });

  it('finds old-writer work after commit without wake capture, including blob readiness and deferred claims', async () => {
    const source = new WorkerWakeReconciliation(pool);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role) VALUES($1,$2,'wake','user')", [raw, vault]);
    await pool.query("INSERT INTO extraction_queue(chunk_id,vault_id,available_at) VALUES($1,$2,now()-interval '1 minute')", [raw, vault]);
    expect(await row()).toBeUndefined();
    expect(await source.dueAt('extraction')).toBeNull();
    // Current accepted source metadata is immutable. Replace the synthetic legacy
    // unready fixture with a newly committed ready source, as a real writer would.
    await pool.query('DELETE FROM extraction_queue WHERE vault_id=$1', [vault]);
    await pool.query('DELETE FROM raw_chunks WHERE id=$1', [raw]);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key) VALUES($1,$2,'wake','user','local','wake-source')", [raw, vault]);
    await pool.query("INSERT INTO extraction_queue(chunk_id,vault_id,available_at) VALUES($1,$2,now()-interval '1 minute')", [raw, vault]);
    const date = await source.dueAt('extraction');
    expect(date!.getTime()).toBeLessThan(Date.now());
    const repairing = new WorkerWakeDispatcher({ enabled: true, store, transport, reconciliation: source, contradictionAllowance: 2, contradictionCycleMs: 30_000 });
    expect((await repairing.dispatchOnce()).outcomes.extraction).toBe('created');
    const active = (await row()).active_token;
    const generation = (await row()).active_generation;
    await completeWorkerWake(pool, { version: 1, kind: 'extraction', token: active, generation });
    // Even an incorrect/unknown acknowledgement cannot strand surviving SQL work.
    expect((await repairing.dispatchOnce()).outcomes.extraction).toBe('created');
    await pool.query("UPDATE extraction_queue SET available_at=now()+interval '1 day', claim_token=gen_random_uuid(), claimed_by='wake-test', claimed_at=now(), lease_expires_at=now()+interval '2 days' WHERE vault_id=$1", [vault]);
    expect((await source.dueAt('extraction'))!.getTime()).toBeGreaterThan(Date.now() + 47 * 3600_000);
  });

  it('stores future work without creating external tasks, advances when due without a new producer', async () => {
    const future = new Date(Date.now() + 86_400_000);
    await store.reconcile({ kind: 'extraction', dueAt: future });
    await store.reconcile({ kind: 'extraction', dueAt: future });
    expect((await row()).requested_generation).toBe('1');
    expect((await dispatch()).outcomes.extraction).toBe('idle');
    expect(transport.create).not.toHaveBeenCalled();
    await pool.query("UPDATE worker_wake_intents SET pending_due_at=now()-interval '1 second'");
    expect((await dispatch()).outcomes.extraction).toBe('created');
  });

  it('advances future pending work when a new immediate request arrives', async () => {
    await request('extraction', new Date('2030-01-01Z'));
    await request();
    expect((await dispatch()).outcomes.extraction).toBe('created');
    expect((await row()).active_generation).toBe('2');
  });

  it('reconciles all maintenance kinds without user traffic and excludes terminal outbox rows', async () => {
    const source = new WorkerWakeReconciliation(pool, { contradictionEnabled: true });
    await pool.query("INSERT INTO memory_contradiction_pending_vaults(vault_id,pending_count,next_visit_at) VALUES($1,1,now()+interval '1 day')", [vault]);
    expect((await source.dueAt('contradiction'))!.getTime()).toBeGreaterThan(Date.now() + 23 * 3600_000);
    expect(await new WorkerWakeReconciliation(pool).dueAt('contradiction')).toBeNull();
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 second' WHERE vault_id=$1", [vault]);
    expect((await source.dueAt('contradiction'))!.getTime()).toBeLessThan(Date.now());
    await pool.query("INSERT INTO vault_usage(vault_id,period) VALUES($1,'2020-01')", [vault]);
    expect((await source.dueAt('usage_periods'))!.getTime()).toBeLessThanOrEqual(Date.now());
    await pool.query("INSERT INTO raw_chunk_blob_write_intents(vault_id,blob_store,blob_key,updated_at) VALUES($1,'local',$2,now()-interval '16 minutes')", [vault, 'wake-'+vault]);
    expect((await source.dueAt('blob_reconciliation'))!.getTime()).toBeLessThan(Date.now());
    // Other integration suites intentionally retain committed platform events.
    // Exercise the actual global reconciliation query against an isolated copy of
    // the real schema, on one retained connection, without rewriting others' data.
    const client = await pool.connect();
    const unrelated = crypto.randomUUID();
    try {
      await pool.query("INSERT INTO platform_event_outbox(event_id,event_type,occurred_at,subject,payload) VALUES($1,'fixture',now(),'isolation-control','{}')", [unrelated]);
      await client.query('CREATE TEMP TABLE platform_event_outbox (LIKE public.platform_event_outbox INCLUDING ALL)');
      const connection = Object.assign(Object.create(client), { query: client.query.bind(client), release: () => {} }) as PoolClient;
      const isolated = new WorkerWakeReconciliation({ connect: async () => connection });
      await client.query("INSERT INTO platform_event_outbox(event_id,event_type,occurred_at,subject,payload,status,next_retry_at) VALUES($1,'fixture',now(),'fixture','{}','failed',now()+interval '1 day')", [event]);
      expect((await isolated.dueAt('platform_outbox'))!.getTime()).toBeGreaterThan(Date.now() + 23 * 3600_000);
      await client.query("UPDATE platform_event_outbox SET status='dead' WHERE event_id=$1", [event]);
      expect(await isolated.dueAt('platform_outbox')).toBeNull();
      await client.query("UPDATE platform_event_outbox SET status='pending',occurred_at=now()+interval '1 day' WHERE event_id=$1", [event]);
      expect((await isolated.dueAt('platform_outbox'))!.getTime()).toBeLessThanOrEqual(Date.now());
      expect((await pool.query('SELECT status FROM platform_event_outbox WHERE event_id=$1',[unrelated])).rows[0].status).toBe('pending');
    } finally {
      client.release(true); // Discard the connection and its temporary table.
      await pool.query('DELETE FROM platform_event_outbox WHERE event_id=$1',[unrelated]);
    }
  });

  it('respects curation cooldowns, leases, disabled plans and UTC monthly budgets', async () => {
    const source = new WorkerWakeReconciliation(pool, { curationEnabled: true });
    await pool.query("INSERT INTO curation_queue(vault_id,work_key,available_at) VALUES($1,'wake-fixture',now()-interval '1 second')", [vault]);
    expect(await new WorkerWakeReconciliation(pool).dueAt('curation')).toBeNull();
    expect((await source.dueAt('curation'))!.getTime()).toBeLessThan(Date.now());
    await pool.query("UPDATE vaults SET rate_limit_override='{\"curator_enabled\":false}' WHERE id=$1", [vault]);
    expect(await source.dueAt('curation')).toBeNull();
    // JSON null must fall back to the plan, just like the actual claimant.
    await pool.query("UPDATE vaults SET rate_limit_override='{\"curator_enabled\":null,\"curator_runs_per_month\":1}' WHERE id=$1", [vault]);
    await pool.query("INSERT INTO vault_usage(vault_id,period,curator_runs) VALUES($1,to_char(now() AT TIME ZONE 'UTC','YYYY-MM'),1)", [vault]);
    const nextMonth = (await pool.query("SELECT (date_trunc('month',now() AT TIME ZONE 'UTC')+interval '1 month') AT TIME ZONE 'UTC' AS next")).rows[0].next;
    expect(await source.dueAt('curation')).toEqual(nextMonth);
    await pool.query("UPDATE vault_usage SET period='2020-01' WHERE vault_id=$1", [vault]);
    expect((await source.dueAt('curation'))!.getTime()).toBeLessThan(Date.now());
    await pool.query("INSERT INTO vault_curation_state(vault_id,next_curator_run_at) VALUES($1,now()+interval '1 hour')", [vault]);
    expect((await source.dueAt('curation'))!.getTime()).toBeGreaterThan(Date.now() + 3500_000);
    await pool.query("UPDATE curation_queue SET claim_token=gen_random_uuid(),claimed_at=now(),claimed_by='wake-test',lease_expires_at=now()+interval '2 hours' WHERE vault_id=$1", [vault]);
    expect((await source.dueAt('curation'))!.getTime()).toBeGreaterThan(Date.now() + 7100_000);
  });

  it('coalesces concurrent high fan-in producers in a consistent lock order', async () => {
    await Promise.all(Array.from({ length: 8 }, async (_, n) => {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const kinds = n % 2 ? [...WORKER_WAKE_KINDS].reverse() : [...WORKER_WAKE_KINDS];
        await requestWorkerWakes(client, Array.from({ length: 600 }, (_, i) => ({ kind: kinds[i % kinds.length], dueAt: due })), true);
        await client.query('COMMIT');
      } finally { await client.query('ROLLBACK'); client.release(); }
    }));
    const rows = (await pool.query('SELECT requested_generation FROM worker_wake_intents')).rows;
    expect(rows).toHaveLength(6);
    expect(rows.every(row => row.requested_generation === '8')).toBe(true);
    expect(Object.values((await dispatch()).outcomes)).toEqual(Array(6).fill('created'));
    expect(transport.create).toHaveBeenCalledTimes(6);
  });

  it('retains successful SQL dispatch state when its commit acknowledgement is lost', async () => {
    await request();
    const actual = store.dispatched.bind(store);
    const mark = vi.spyOn(store, 'dispatched').mockImplementationOnce(async reservation => {
      await actual(reservation);
      throw new Error('lost COMMIT acknowledgement');
    });
    try {
      expect((await dispatch()).outcomes.extraction).toBe('failed');
      expect((await row()).dispatched).toBe(true);
      await ready();
      expect((await dispatch()).outcomes.extraction).toBe('retained');
      expect(transport.create).toHaveBeenCalledOnce();
      expect((await row()).completed_generation).toBe('0');
    } finally { mark.mockRestore(); }
  });

  it('refuses schema removal while an active cycle can still be acknowledged', async () => {
    await request();
    await store.reserve('extraction', 2, 30_000);
    await expect(pool.query(fs.readFileSync(path.resolve(__dirname, '../db/migrations/down/063_worker_wake_intents.sql'), 'utf8')))
      .rejects.toThrow('Drain active wake cycles');
    expect((await row()).active_generation).toBe('1');
  });

  it('supports additive upgrade/down/reapply without changing business rows', async () => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const before = (await client.query('SELECT * FROM vaults WHERE id=$1', [vault])).rows[0];
      await client.query(fs.readFileSync(path.resolve(__dirname, '../db/migrations/down/063_worker_wake_intents.sql'), 'utf8'));
      await client.query(fs.readFileSync(path.resolve(__dirname, '../db/migrations/063_worker_wake_intents.sql'), 'utf8'));
      expect((await client.query('SELECT * FROM vaults WHERE id=$1', [vault])).rows[0]).toEqual(before);
      await request('extraction', due, client);
      expect((await client.query('SELECT kind FROM worker_wake_intents')).rows).toEqual([{ kind: 'extraction' }]);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  it('rejects malformed persistent state, including partially NULL active snapshots', async () => {
    await request();
    await expect(pool.query("UPDATE worker_wake_intents SET active_token=gen_random_uuid()")).rejects.toThrow();
    await expect(pool.query("UPDATE worker_wake_intents SET completed_generation=2")).rejects.toThrow();
    await expect(pool.query("INSERT INTO worker_wake_intents(kind) VALUES ('unknown')")).rejects.toThrow();
    expect(WORKER_WAKE_KINDS).toHaveLength(6);
  });
});
