import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { Worker } from 'node:worker_threads';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { WorkerExecutionStore } from '../store';
import { WorkerWakeStore, requestWorkerWakes } from '../../services/worker-wake-store';
import { superviseExecution, type ExecutionInvocation } from '../supervisor';
import type { WorkerWakeKind } from '../../services/worker-wake-types';

const url = process.env.PERSISTIO_TEST_DATABASE_URL;
const built = path.resolve(__dirname, '../../..', 'dist');
describe.skipIf(!url)('actual request worker cores with local HTTP provider (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: url });
  const store = new WorkerExecutionStore(pool), wakes = new WorkerWakeStore(pool);
  let root: string, provider: http.Server, baseURL: string;
  const vaults: string[] = [], queues: string[] = [];
  const measurements: Array<{ kind: string; elapsedMs: number; providerCalls: number; peakConnections: number }> = [];
  let behavior: 'success' | 'hang' | 'unauthorized' = 'success';
  let calls = 0, closed = 0;
  let vector: number[];
  const fact = { fact: 'The project uses PostgreSQL for durable storage.', subject: 'Project storage', score: 9,
    salience: 0.9, sensitivity: 'low', type: 'system_fact', scope: 'project', polarity: 'neutral', volatility: 'low',
    evidence: 'Project uses PostgreSQL', source_refs: ['S1'], scope_basis: 'Explicit project context', valid_from: null, valid_until: null };
  beforeAll(async () => {
    if (!url || url !== process.env.DATABASE_URL || !/^persistio_test_/.test(new URL(url).pathname.slice(1))) throw new Error('Use matching isolated persistio_test_ URLs');
    const db = await import('../../db/client'); try { await db.runMigrations(); } finally { await db.closePool(); }
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector = [1, ...Array(Number(/\((\d+)\)/.exec(type)![1]) - 1).fill(0)];
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'persistio-pr3-core-'));
    provider = http.createServer(async (request, response) => {
      let raw = ''; for await (const chunk of request) raw += chunk;
      calls++; response.on('close', () => { closed++; });
      if (behavior === 'hang') return;
      if (behavior === 'unauthorized') { response.writeHead(401).end(JSON.stringify({ error: { message: 'fixture unauthorized' } })); return; }
      const body = JSON.parse(raw); let result: unknown;
      if (request.url?.endsWith('/embeddings')) {
        result = { data: body.input.map((_text: string, index: number) => ({ index, embedding: vector })), usage: { prompt_tokens: 10, total_tokens: 10 } };
      } else {
        const name = body.response_format?.json_schema?.name;
        let content: string;
        if (name?.startsWith('extracted_')) content = JSON.stringify({ facts: [fact] });
        else if (name?.includes('curat')) {
          const candidates = JSON.parse(body.messages[1].content[0].text).memories;
          content = JSON.stringify({ schema_version: 'curation-plan.v2', keep: candidates.map((m: any) => ({ id: m.id, reason: 'Useful supported fact' })), update: [], consolidate: [], archive: [], edges: [], scope_changes: [] });
        } else content = body.messages[0].content.includes('Identify entities') ? '[]' : 'project storage';
        result = { choices: [{ finish_reason: 'stop', message: { role: 'assistant', content } }], usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 } };
      }
      response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(result));
    });
    await new Promise<void>(resolve => provider.listen(0, '127.0.0.1', resolve));
    baseURL = 'http://127.0.0.1:' + (provider.address() as import('node:net').AddressInfo).port + '/v1/';
  });
  beforeEach(async () => {
    behavior = 'success'; calls = 0; closed = 0;
    await pool.query("UPDATE worker_execution_slot SET owner=NULL,kind=NULL,token=NULL,generation=NULL,expires_at=NULL,runtime_state='{}'::jsonb WHERE id=1");
    await pool.query('DELETE FROM worker_wake_intents');
  });
  afterEach(async () => {
    provider.closeAllConnections();
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])', [queues]);
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]);
    queues.length = 0; vaults.length = 0;
  });
  afterAll(async () => {
    await pool.query("UPDATE worker_execution_slot SET owner=NULL,kind=NULL,token=NULL,generation=NULL,expires_at=NULL,runtime_state='{}'::jsonb WHERE id=1");
    await pool.query('DELETE FROM worker_wake_intents');
    await pool.end(); provider.closeAllConnections(); await new Promise<void>(resolve => provider.close(() => resolve()));
    await fs.rm(root, { recursive: true, force: true });
    // Optional, explicitly requested diagnostic artifact; never source content or credentials.
    if (process.env.PR3_MEASUREMENTS_PATH) await fs.writeFile(process.env.PR3_MEASUREMENTS_PATH, JSON.stringify(measurements, null, 2));
  });
  async function fixture(kind: 'extraction' | 'curation', override: Record<string, number> = {}) {
    const vault = crypto.randomUUID(), segment = crypto.randomUUID(), queue = crypto.randomUUID(), chunk = crypto.randomUUID();
    vaults.push(vault); queues.push(queue);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id,rate_limit_override) VALUES($1,'request-core',$2,'unlimited',$3::jsonb)", [vault, crypto.randomUUID(), JSON.stringify(override)]);
    await fs.writeFile(path.join(root, chunk), fact.fact);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,capture_context) VALUES($1::uuid,$2,'request-core','user','local',$1::text,100,'{\"session_id\":\"request-core\",\"project_id\":\"project\"}')", [chunk, vault]);
    await pool.query("INSERT INTO segments(id,vault_id,session_id,project_id,chunk_ids,context) VALUES($1,$2,'request-core','project',$3::uuid[],'Project storage facts')", [segment, vault, [chunk]]);
    if (kind === 'extraction') await pool.query('INSERT INTO extraction_queue(id,vault_id,segment_id) VALUES($1,$2,$3)', [queue, vault, segment]);
    else {
      const memory = crypto.randomUUID();
      await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,scope,scope_key,status,source_segment_id,type,confidence,salience,embedding) VALUES($1,$2,$3,'Project storage',$4,'project','project','active',$5,'system_fact',0.9,0.8,$6::vector)", [memory, vault, fact.fact, crypto.randomUUID(), segment, JSON.stringify(vector)]);
      await pool.query('INSERT INTO curation_queue(id,vault_id,segment_id,work_key) VALUES($1,$2,$3,$4)', [queue, vault, segment, crypto.randomUUID()]);
      await pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) SELECT $1,vault_id,id,revision FROM memories WHERE id=$2', [queue, memory]);
    }
    return { vault, queue };
  }
  async function execute(kind: WorkerWakeKind, timeout = 15000) {
    const client = await pool.connect();
    try { await client.query('BEGIN'); await requestWorkerWakes(client, [{ kind, dueAt: new Date(0) }], true); await client.query('COMMIT'); }
    finally { client.release(); }
    const wake = (await wakes.reserve(kind, 2, 30000))!.wake, owner = crypto.randomUUID();
    expect((await store.acquire(owner, wake)).status).toBe('acquired');
    // Substitute only the HTTP boundary. The real OpenAI embedder intentionally
    // fixes its production host; never permit this fixture to contact that host.
    const factory = (invocation: ExecutionInvocation) => new Worker(`
      const {workerData}=require('node:worker_threads'); const transport=global.fetch;
      global.fetch=(input,init)=>{const u=new URL(typeof input==='string'?input:input.url??input);
        if(u.hostname==='api.openai.com')return transport(workerData.provider+u.pathname.replace(/^\\/v1\\//,''),init);
        if(u.origin!==new URL(workerData.provider).origin)throw new Error('Unexpected fixture network destination');
        return transport(input,init);
      };
      const runtime=require('openai/_shims/node-runtime').getRuntime();
      require('openai/_shims/registry').setShims({...runtime,fetch:global.fetch});
      require(workerData.entry);`, {
      eval: true, execArgv: [], workerData: { ...invocation, entry: path.join(built, 'request-worker/execution-thread.js'), provider: baseURL }, env: { ...process.env, DATABASE_URL: url!, PERSISTIO_MODE: 'request-worker',
        ENCRYPTION_ENABLED: 'false', TELEMETRY_PROVIDER: 'none', CUSTOMER_METRICS_PUBLISHER: 'noop',
        EXTRACTION_BASE_URL: '', EXTRACTION_API_KEY: '', EXTRACTION_MODEL: '', ESCALATION_BASE_URL: '', ESCALATION_API_KEY: '', ESCALATION_MODEL: '',
        EXTRACTOR_BASE_URL: baseURL, EXTRACTOR_API_KEY: 'fixture', EXTRACTOR_MODEL: 'fixture', OPENAI_API_KEY: 'fixture',
        CURATOR_BASE_URL: baseURL, CURATOR_API_KEY: 'fixture', CURATOR_MODEL: 'fixture', CURATOR_AUTO_RUN: kind === 'curation' ? 'true' : 'false',
        CONTRADICTION_SCAN_ENABLED: 'false', RAW_CHUNK_STORAGE_PROVIDER: 'local', RAW_CHUNK_LOCAL_DIR: root,
        STORAGE_EMBEDDING_DIMENSIONS: String(vector.length), CIRCUIT_BREAKER_THRESHOLD: '1'
      }
    });
    const start = performance.now(), initialCalls = calls; let peakConnections = 0;
    let sampling = Promise.resolve();
    const sampler = setInterval(() => { sampling = sampling.then(async () => {
      const count = Number((await pool.query('SELECT count(*) FROM pg_stat_activity WHERE application_name=$1', ['persistio:req:' + owner])).rows[0].count);
      peakConnections = Math.max(peakConnections, count);
    }); }, 10);
    let success: boolean;
    try { success = await superviseExecution({ id: owner, wake, deadline: Date.now() + timeout }, store, new AbortController().signal, factory); }
    finally { clearInterval(sampler); await sampling; }
    await store.clearBackends(owner); await store.finish(owner, wake, success);
    measurements.push({ kind, elapsedMs: Math.round(performance.now() - start), providerCalls: calls - initialCalls, peakConnections });
    expect(peakConnections).toBeLessThanOrEqual(2);
    return success;
  }
  it.each(['extraction', 'curation'] as const)('commits one real %s unit through SQL fences and local provider transport', async kind => {
    const f = await fixture(kind);
    expect(await execute(kind)).toBe(true);
    expect(calls).toBeGreaterThan(0);
    expect((await pool.query(`SELECT 1 FROM ${kind}_queue WHERE id=$1`, [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM memories WHERE vault_id=$1', [f.vault])).rowCount).toBe(1);
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBeGreaterThan(0);
  }, 20000);
  it('cancels a real hanging model request without a business retry or late commit', async () => {
    const f = await fixture('extraction'); behavior = 'hang';
    expect(await execute('extraction', 1500)).toBe(false);
    expect(calls).toBe(1); await vi.waitFor(() => expect(closed).toBe(1));
    expect((await pool.query('SELECT retry_count FROM extraction_queue WHERE id=$1', [f.queue])).rows[0].retry_count).toBe(0);
    expect((await pool.query('SELECT 1 FROM memories WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
    const state = (await pool.query('SELECT runtime_state FROM worker_execution_slot')).rows[0].runtime_state;
    expect(Object.keys(state['ai-requests'])).toContain(f.vault + ':extraction:requests');
  }, 10000);
  it('preserves AI budget debt across disposable threads', async () => {
    const f = await fixture('extraction', { ai_requests_per_minute: 1 });
    expect(await execute('extraction')).toBe(true); // bounded pass defers after first paid call
    expect(calls).toBe(1);
    await pool.query('UPDATE extraction_queue SET available_at=now() WHERE id=$1', [f.queue]);
    expect(await execute('extraction')).toBe(true);
    expect(calls).toBe(1);
    expect((await pool.query('SELECT retry_count FROM extraction_queue WHERE id=$1', [f.queue])).rows[0].retry_count).toBe(0);
  }, 20000);
  it('preserves an opened authentication circuit across disposable threads', async () => {
    const first = await fixture('extraction'); behavior = 'unauthorized';
    expect(await execute('extraction')).toBe(true); expect(calls).toBe(1);
    await pool.query('UPDATE extraction_queue SET available_at=now() WHERE id=$1', [first.queue]);
    expect(await execute('extraction')).toBe(true); expect(calls).toBe(1);
  }, 20000);
});
