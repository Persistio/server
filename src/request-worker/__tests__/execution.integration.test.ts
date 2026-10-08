import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { Client, Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { WorkerExecutionStore } from '../store';
import { WorkerWakeStore, requestWorkerWakes } from '../../services/worker-wake-store';
import type { WorkerWakeKind } from '../../services/worker-wake-types';
import { superviseExecution, type ExecutionInvocation } from '../supervisor';

const url = process.env.PERSISTIO_TEST_DATABASE_URL;
const built = path.resolve(__dirname, '../../..', 'dist');
describe.skipIf(!url)('request worker ownership and real thread execution (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: url, max: 4 });
  const store = new WorkerExecutionStore(pool), wakes = new WorkerWakeStore(pool);
  const vaults: string[] = [];
  async function wake(kind: WorkerWakeKind = 'extraction') {
    const client = await pool.connect();
    try { await client.query('BEGIN'); await requestWorkerWakes(client, [{ kind, dueAt: new Date(0) }], true); await client.query('COMMIT'); }
    finally { client.release(); }
    return (await wakes.reserve(kind, 2, 30000))!.wake;
  }
  const clearSlot = () => pool.query('UPDATE worker_execution_slot SET owner=NULL,kind=NULL,token=NULL,generation=NULL,expires_at=NULL WHERE id=1');
  const active = async () => (await pool.query('SELECT * FROM worker_execution_slot WHERE id=1')).rows[0];
  beforeAll(async () => {
    if (!url || url !== process.env.DATABASE_URL || !/^persistio_test_/.test(new URL(url).pathname.slice(1))) throw new Error('Use matching isolated persistio_test_ URLs');
    const db = await import('../../db/client');
    try { await db.runMigrations(); } finally { await db.closePool(); }
    if (!fs.existsSync(path.join(built, 'request-worker/execution-thread.js'))) throw new Error('Run npm run build before real-thread tests');
  });
  beforeEach(async () => { await clearSlot(); await pool.query('DELETE FROM worker_wake_intents'); });
  afterAll(async () => {
    await clearSlot(); await pool.query('DELETE FROM worker_wake_intents');
    await pool.query("DELETE FROM platform_event_outbox WHERE payload->>'vault_id'=ANY($1::text[])", [vaults]);
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]); await pool.end();
  });
  it('admits exactly one owner across duplicate deliveries and different wake kinds', async () => {
    const extraction = await wake(), usage = await wake('usage_periods');
    const results = await Promise.all(Array.from({ length: 12 }, (_, n) => store.acquire(crypto.randomUUID(), n % 2 ? extraction : usage)));
    expect(results.filter(result => result.status === 'acquired')).toHaveLength(1);
    expect(results.filter(result => result.status === 'busy')).toHaveLength(11);
  });
  it('rejects stale tokens/generations and fences takeover against renewal and completion', async () => {
    const current = await wake(), oldOwner = crypto.randomUUID(), newOwner = crypto.randomUUID();
    expect(await store.acquire(oldOwner, { ...current, token: crypto.randomUUID() })).toEqual({ status: 'stale' });
    expect(await store.acquire(oldOwner, { ...current, generation: '2' })).toEqual({ status: 'stale' });
    expect((await store.acquire(oldOwner, current)).status).toBe('acquired');
    await pool.query("UPDATE worker_execution_slot SET expires_at=now()-interval '1 second'");
    expect(await store.acquire(newOwner, current)).toEqual({ status: 'acquired', previousOwner: oldOwner });
    expect(await store.renew(oldOwner)).toBe(false);
    expect(await store.finish(oldOwner, current, true)).toBe(false);
    expect((await active()).owner).toBe(newOwner);
    expect(await store.renew(newOwner)).toBe(true);
    expect(await store.finish(newOwner, current, true)).toBe(true);
    expect((await active()).owner).toBeNull();
    expect(await store.acquire(crypto.randomUUID(), current)).toEqual({ status: 'stale' });
  });
  it('preserves a producer generation racing completion and never acknowledges a rotated identity', async () => {
    const current = await wake(), owner = crypto.randomUUID(); await store.acquire(owner, current);
    const producer = await pool.connect();
    try {
      await producer.query('BEGIN');
      await requestWorkerWakes(producer, [{ kind: 'extraction', dueAt: new Date(0) }], true);
      await producer.query('COMMIT');
    } finally { producer.release(); }
    expect(await store.finish(owner, current, true)).toBe(true);
    const row = (await pool.query("SELECT * FROM worker_wake_intents WHERE kind='extraction'")).rows[0];
    expect(row.completed_generation).toBe('1'); expect(row.requested_generation).toBe('2'); expect(row.pending_due_at).not.toBeNull();
    const next = (await wakes.reserve('extraction', 2, 30000))!.wake;
    await store.acquire(owner, next);
    await pool.query("UPDATE worker_wake_intents SET active_token=gen_random_uuid() WHERE kind='extraction'");
    expect(await store.renew(owner)).toBe(false);
    expect(await store.finish(owner, next, true)).toBe(false);
    expect((await active()).owner).toBeNull();
  });
  it('failure releases only execution admission and leaves wake identity retryable', async () => {
    const current = await wake(), owner = crypto.randomUUID(); await store.acquire(owner, current);
    expect(await store.finish(owner, current, false)).toBe(false);
    expect((await store.acquire(crypto.randomUUID(), current)).status).toBe('acquired');
  });
  it('terminates the exact old backend, rolls back in-flight writes and frees session locks', async () => {
    const owner = crypto.randomUUID();
    const target = new Client({ connectionString: url, application_name: 'persistio:req:' + owner });
    const unrelated = new Client({ connectionString: url, application_name: 'persistio:req:' + crypto.randomUUID() });
    target.on('error', () => {}); unrelated.on('error', () => {});
    await target.connect(); await unrelated.connect();
    const pid = (await target.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
    try {
      await target.query('BEGIN'); await target.query('SELECT pg_advisory_lock(834561234)');
      await target.query("INSERT INTO worker_wake_intents(kind,requested_generation,pending_due_at) VALUES('usage_periods',1,now())");
      const sleeping = target.query('SELECT pg_sleep(30)').catch(error => error);
      await vi.waitFor(async () => expect((await pool.query('SELECT wait_event FROM pg_stat_activity WHERE pid=$1', [pid])).rows[0]?.wait_event).toBe('PgSleep'));
      await store.clearBackends(owner); expect(await sleeping).toBeInstanceOf(Error);
      expect((await pool.query('SELECT 1 FROM worker_wake_intents')).rowCount).toBe(0);
      expect((await unrelated.query('SELECT pg_try_advisory_lock(834561234) AS acquired')).rows[0].acquired).toBe(true);
      expect((await unrelated.query('SELECT 1')).rowCount).toBe(1);
    } finally { await target.end(); await unrelated.end(); }
  });
  it('real execution context refuses a post-cancellation commit and retry update', async () => {
    const owner = crypto.randomUUID();
    const outcome = await new Promise<any>((resolve, reject) => {
      const thread = new Worker(`
        const {workerData,parentPort}=require('node:worker_threads');
        const {Client}=require('pg'); const context=require(workerData.context);
        const cancel=new AbortController(); context.installExecutionContext(workerData.id,cancel.signal,Date.now()+10000);
        const db=new Client({connectionString:workerData.url,application_name:context.executionApplicationName('fallback')}); context.guardExecutionClient(db);
        (async()=>{await db.connect(); await db.query('BEGIN');
          await db.query("INSERT INTO worker_wake_intents(kind,requested_generation,pending_due_at) VALUES('usage_periods',1,now())");
          cancel.abort(new Error('cancelled')); let prevented=0;
          for(const sql of ['COMMIT',"UPDATE worker_wake_intents SET attempts=attempts+1"]) {try{await db.query(sql)}catch{prevented++}}
          await db.query('ROLLBACK'); await db.end(); parentPort.postMessage({prevented});
        })().catch(e=>{throw e});`, { eval: true, workerData: { id: owner, url, context: path.join(built, 'request-worker/context.js') } });
      thread.once('message', resolve); thread.once('error', reject);
    });
    expect(outcome.prevented).toBe(2); await store.clearBackends(owner);
    expect((await pool.query('SELECT 1 FROM worker_wake_intents')).rowCount).toBe(0);
  });
  it('runs the actual bounded usage worker, leaves remaining durable work, and records one closure', async () => {
    for (let n = 0; n < 2; n++) {
      const vault = crypto.randomUUID(); vaults.push(vault);
      await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id,account_id) VALUES($1,'request-unit',$2,'unlimited',$3)", [vault, crypto.randomUUID(), crypto.randomUUID()]);
      await pool.query("INSERT INTO vault_usage(vault_id,period,ingest_events) VALUES($1,'2000-01',3)", [vault]);
    }
    const current = await wake('usage_periods'), owner = crypto.randomUUID(); await store.acquire(owner, current);
    const start = performance.now();
    const factory = (invocation: ExecutionInvocation) => new Worker(path.join(built, 'request-worker/execution-thread.js'), {
      execArgv: [], workerData: invocation, env: { ...process.env, DATABASE_URL: url!, PERSISTIO_MODE: 'request-worker', ENCRYPTION_ENABLED: 'false', TELEMETRY_PROVIDER: 'none', CUSTOMER_METRICS_PUBLISHER: 'noop' }
    });
    expect(await superviseExecution({ id: owner, wake: current, deadline: Date.now() + 10000 }, store, new AbortController().signal, factory)).toBe(true);
    await store.clearBackends(owner); expect(await store.finish(owner, current, true)).toBe(true);
    expect((await pool.query("SELECT 1 FROM vault_usage WHERE vault_id=ANY($1::uuid[]) AND period='2000-01'", [vaults])).rowCount).toBe(1);
    expect((await pool.query("SELECT 1 FROM platform_event_outbox WHERE payload->>'vault_id'=ANY($1::text[])", [vaults])).rowCount).toBe(1);
    console.info('PR3 actual usage unit elapsed ms:', Math.round(performance.now() - start));
  }, 15000);
  it('refuses rollback of the admission migration while an owner is present', async () => {
    const current = await wake(); await store.acquire(crypto.randomUUID(), current);
    await expect(pool.query(fs.readFileSync(path.join(built, 'db/migrations/down/064_worker_execution_slot.sql'), 'utf8'))).rejects.toThrow();
    expect((await pool.query('SELECT 1 FROM worker_execution_slot')).rowCount).toBe(1);
  });
  it('fences a stale state checkpoint before model dispatch and invalidates catch-path writes', async () => {
    const current = await wake(), owner = crypto.randomUUID(); await store.acquire(owner, current);
    const thread = new Worker(`
      const {workerData,parentPort}=require('node:worker_threads'); const {Pool}=require('pg');
      const context=require(workerData.context),state=require(workerData.state);
      context.installExecutionContext(workerData.owner,new AbortController().signal,Date.now()+10000);
      const db=new Pool({connectionString:workerData.url});
      (async()=>{await state.initializeExecutionState(db,workerData.owner);
        state.registerExecutionState('fixture',()=>{},()=>({debt:1}));
        parentPort.postMessage('ready');
        await new Promise(resolve=>parentPort.once('message',resolve));
        let rejected=false,invalid=false;
        try{await state.checkpointExecutionState()}catch{rejected=true}
        try{context.assertExecutionLive()}catch{invalid=true}
        await db.end();parentPort.postMessage({rejected,invalid});parentPort.close();
      })().catch(e=>{throw e});`, { eval: true, workerData: { owner, url,
        context: path.join(built, 'request-worker/context.js'), state: path.join(built, 'request-worker/state.js') } });
    const exited = new Promise(resolve => thread.once('exit', resolve));
    try {
      await new Promise<void>((resolve, reject) => { thread.once('message', () => resolve()); thread.once('error', reject); });
      await pool.query("UPDATE worker_execution_slot SET expires_at=now()-interval '1 second'");
      const replacement = crypto.randomUUID(); await store.acquire(replacement, current);
      const result = new Promise(resolve => thread.once('message', resolve)); thread.postMessage('continue');
      expect(await result).toEqual({ rejected: true, invalid: true });
      expect((await active()).runtime_state).not.toHaveProperty('fixture');
      expect((await active()).owner).toBe(replacement);
    } finally { await thread.terminate(); await exited; }
  });
});
