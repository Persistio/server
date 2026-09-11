import crypto from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { parse } from 'yaml';
import Fastify, { type FastifyRequest } from 'fastify';
import { Pool, type PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

// Only external identity/quota/embedding services are stubbed. Selection,
// provenance, projection, built plugin validation/rendering and ACK SQL are real.
const state = vi.hoisted(() => ({ pool: null as Pool | null, vaultId: '', policy: 'approved_only',
  committed:false, failCommit:false, throwMetrics:false, publications:[] as Array<{value:number;stage:unknown;committed:boolean}>,
  vector: Array.from({ length: 1536 }, (_, i) => i === 0 ? 1 : 0) }));
vi.mock('../metrics',async importOriginal=>{
  const actual=await importOriginal<Record<string,unknown>>();
  return {...actual,...Object.fromEntries(['memoryPolicyEventCounter','recallDeliveryCounter','recallDeliveryMissingAckCounter','globalRuleDeliveryCounter'].map(name=>[name,{
    add:(value:number,attributes:Record<string,unknown>)=>{state.publications.push({value,stage:attributes?.stage,committed:state.committed});if(state.throwMetrics)throw Error('fixture publisher');}
  }]))};
});
vi.mock('../middleware/auth', () => ({ requireVaultReadAuth: async (request: FastifyRequest) => {
  request.vault = { id: state.vaultId, encrypted_dek: null, vault_encryption_enabled: false } as FastifyRequest['vault'];
} }));
vi.mock('../config', () => ({ getConfig: () => ({ DEFAULT_RECALL_TOP_K: 8, MIN_RECALL_SIMILARITY: 0.3, GLOBAL_RULE_POLICY: state.policy }) }));
vi.mock('../services/usage', () => ({ consumeApiQuota: vi.fn(async () => ({})), applyRateLimitHeaders: vi.fn() }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed: async () => state.vector }) }));
vi.mock('../services/raw-chunk-storage', () => ({ getRawChunkStorage: () => ({ get: vi.fn() }) }));
vi.mock('../db/client', () => ({
  query: (sql: string, params: unknown[]) => state.pool!.query(sql, params),
  withTransaction: async (run: (client: PoolClient) => Promise<unknown>) => {
    const client = await state.pool!.connect();
    try { state.committed=false; await client.query('BEGIN'); const result = await run(client); if(state.failCommit)throw Error('fixture commit failure'); await client.query('COMMIT'); state.committed=true; return result; }
    catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  },
}));
import { registerRecallRoutes } from './recall';
import plugin from '../../../plugin/dist/index.js';
import { PersistioClient, recallBundleMemories } from '../../../plugin/dist/client.js';
import { resolveConfig } from '../../../plugin/dist/config.js';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const require = createRequire(import.meta.url);
const ajv = new (require('ajv/dist/2020'))({ strict: true });
require('ajv-formats')(ajv);
const api = parse(readFileSync(new URL('../../../../openapi.yaml', import.meta.url), 'utf8'));
for (const [name, schema] of Object.entries(api.components.schemas)) ajv.addSchema(schema, `#/components/schemas/${name}`);
const responseContract = ajv.compile(api.paths['/v1/recall'].post.responses['200'].content['application/json'].schema);
const ackContract = ajv.getSchema('#/components/schemas/RenderedDeliveryResponse');
describe.skipIf(!databaseUrl)('server → built plugin → immutable recall delivery (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const app = Fastify();
  const originalFetch = globalThis.fetch;
  const ids: Record<string, string> = {};
  const context = { project_id: 'persistio', agent_id: 'main', trigger_type: 'direct' as const };
  const responses: any[] = [];
  const acknowledgements: any[] = [];
  const cfg = resolveConfig({ baseURL: 'https://persistio.test', apiKey: 'test', recall: {
    maxResults: 8, includeGlobalRules: true, includeRelated: true, includePending: true,
  } });

  async function addMemory(name: string, type: string, scope: string, status = 'active', embedding = true, proof = true, binding = 'persistio') {
    const id = crypto.randomUUID();
    ids[name] = id;
    await pool.query(`INSERT INTO memories (id, vault_id, data, subject, hash, type, scope, scope_key, status, source_chunks)
      VALUES ($1, $2, $3, 'recall contract', $4, $5, $6, $7, $8, NULL)`,
    [id, state.vaultId, `contract ${name}`, crypto.randomUUID(), type, scope, scope === 'global' ? null : binding, status]);
    await pool.query(`UPDATE memories SET authority_state = 'approved', authority_version = 2,
      approved_by = 'vault:test', approved_at = now(), approval_source = 'api' WHERE id = $1`, [id]);
    if (proof) await pool.query(`INSERT INTO memory_authority_events
      (vault_id, memory_id, event_type, new_state, new_version, actor_type, actor_id, source, reason)
      VALUES ($1, $2, 'approve', 'approved', 2, 'user', 'vault:test', 'api', 'recall contract test')`, [state.vaultId, id]);
    if (embedding) await pool.query('INSERT INTO memory_embeddings (memory_id, embedding) VALUES ($1, $2::vector)', [id, JSON.stringify(state.vector)]);
    return id;
  }

  beforeAll(async () => {
    state.pool = pool; state.vaultId = crypto.randomUUID();
    const migration = await pool.query("SELECT 1 FROM schema_migrations WHERE filename = '054_inverted_validity_quarantine.sql'");
    // Selection and delivery require the released schema; do not mutate it here.
    expect(migration.rowCount).toBe(1);
    await pool.query('INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)', [state.vaultId, 'recall-contract', crypto.randomUUID()]);
    await addMemory('global', 'user_rule', 'global');
    await addMemory('direct', 'system_fact', 'project');
    await addMemory('candidate', 'system_fact', 'project', 'candidate');
    await addMemory('graph', 'workflow', 'project', 'active', false);
    await addMemory('missing-proof', 'constraint', 'project', 'active', true, false);
    await addMemory('wrong-scope', 'system_fact', 'project', 'active', true, true, 'other-project');
    await pool.query("INSERT INTO memory_edges (vault_id, from_memory_id, to_memory_id, type) VALUES ($1, $2, $3, 'supports')", [state.vaultId, ids.direct, ids.graph]);
    await registerRecallRoutes(app);
    globalThis.fetch = async (url, init) => {
      const target = new URL(String(url));
      const response = await app.inject({ method: 'POST', url: target.pathname + target.search, payload: JSON.parse(String(init!.body)) });
      expect(response.statusCode, response.body).toBe(200);
      const body = response.json();
      const validate = target.pathname.endsWith('/rendered') ? ackContract : responseContract;
      expect(validate(body), JSON.stringify(validate.errors)).toBe(true);
      if (target.pathname.endsWith('/rendered')) acknowledgements.push(JSON.parse(String(init!.body)));
      else responses.push(body);
      return new Response(response.body, { status: response.statusCode, headers: { 'content-type': 'application/json' } });
    };
  });
  afterAll(async () => {
    globalThis.fetch = originalFetch;
    await app.close();
    await pool.query('DELETE FROM vaults WHERE id = $1', [state.vaultId]);
    await pool.end();
  });

  it.each(['auto', 'memory_recall', 'memory_forget'])('round trips null chunks in all lanes, with exact %s output/ACK', async (kind) => {
    const tools = new Map<string, any>(); const hooks = new Map<string, any>();
    plugin.register({
      pluginConfig: { baseURL: 'https://persistio.test', apiKey: 'test', context: { projectId: 'persistio', agentId: 'main' },
        recall: { maxResults: 8, includeGlobalRules: true, includeRelated: true, includePending: true, tokenBudget: 10000 } },
      registerTool(registration: any, options: any) { tools.set(options.name, typeof registration === 'function' ? registration({ sessionKey: 'C123-topic-456' }) : registration); },
      on(event: string, handler: any) { hooks.set(event, handler); },
    });
    const result = kind === 'auto' ? await hooks.get('before_prompt_build')({ prompt: 'contract' }, { sessionKey: 'C123-topic-456' })
      : await tools.get(kind).execute('contract', { query: 'contract' });
    const text = kind === 'auto' ? result?.prependContext : result.content[0].text;
    for (const name of ['global', 'direct', 'candidate', 'graph']) expect(text).toContain(`contract ${name}`);
    expect(text).not.toContain('contract missing-proof'); expect(text).not.toContain('contract wrong-scope');
    const response = responses.at(-1);
    const memories = Object.values(response.sections).flat() as any[];
    expect(memories).toHaveLength(4);
    expect(memories.every(m => Array.isArray(m.provenance.source_chunk_ids) && m.provenance.source_chunk_ids.length === 0)).toBe(true);
    expect(memories.every(m => m.authority.approved_by === 'vault:test')).toBe(true);
    const outcome = acknowledgements.at(-1);
    expect(outcome.rendered_ids.sort()).toEqual([ids.global, ids.direct, ids.candidate, ids.graph].sort());
    expect(outcome.dropped).toEqual([]);
    const events = await pool.query('SELECT stage, memory_id FROM memory_delivery_events WHERE delivery_id = $1', [response.delivery.id]);
    for (const stage of ['selected', 'returned', 'rendered']) expect(events.rows.filter(row => row.stage === stage).map(row => row.memory_id).sort()).toEqual(outcome.rendered_ids);
  });

  it('checks null-source candidate age on the server, including created_at fallback', async () => {
    const client = new PersistioClient(cfg);
    const current = await client.recallStructuredBundle('contract', context);
    expect(current.sections.candidates.map(m => m.id)).toEqual([ids.candidate]);
    await pool.query("UPDATE memories SET created_at = now() - interval '49 hours' WHERE id = $1", [ids.candidate]);
    try { expect((await client.recallStructuredBundle('contract', context)).sections.candidates).toEqual([]); }
    finally { await pool.query('UPDATE memories SET created_at = now() WHERE id = $1', [ids.candidate]); }
  });

  it.each(['json', 'bundle', 'bundle_v2'])('reconciles every %s lane with receipt, ledger and durable ACK', async format => {
    for (const [scenario, options, expected] of [
      ['empty', { context: { project_id: 'absent' } }, []],
      ['direct', { context }, [ids.direct]],
      ['global-only', { context: { ...context, project_id: 'absent' }, include_global_rules: true }, [ids.global]],
      ['graph', { context, include_related: true }, [ids.direct, ids.graph]],
      ['candidate', { context, include_pending: true }, [ids.direct, ids.candidate]],
      ['mixed', { context, include_global_rules: true, include_related: true, include_pending: true }, [ids.global, ids.direct, ids.graph, ids.candidate]],
      ['scheduled', { context: { ...context, trigger_type: 'scheduled' }, include_global_rules: true }, [ids.direct]],
    ] as const) {
      const result = await app.inject({ method: 'POST', url: `/v1/recall${format === 'json' ? '' : `?format=${format}`}`,
        payload: { query: 'contract', top_k: 8, include_related: false, ...options } });
      expect(result.statusCode, result.body).toBe(200);
      const body = result.json();
      expect(responseContract(body), `${scenario}: ${JSON.stringify(responseContract.errors)}`).toBe(true);
      const withoutReceipt={...body};delete withoutReceipt.delivery;
      expect(responseContract(withoutReceipt), 'receipt is mandatory in every documented response').toBe(false);
      const presented = format === 'json' ? [...body.memories, ...body.related_memories].map(m => m.id)
        : format === 'bundle' ? [...Object.values(body.bundle_ids).flat(), ...Object.values(body.related_bundle_ids ?? {}).flat()]
        : (Object.values(body.sections).flat() as any[]).map(m => m.id);
      expect(presented.sort(), scenario).toEqual([...expected].sort());
      expect([...body.delivery.selected_ids].sort()).toEqual(presented);
      const events = (await pool.query('SELECT memory_id,stage FROM memory_delivery_events WHERE delivery_id=$1', [body.delivery.id])).rows;
      for (const stage of ['selected', 'returned']) expect(events.filter(e => e.stage === stage).map(e => e.memory_id).sort()).toEqual(presented);
      const ack = await app.inject({ method: 'POST', url: `/v1/recall/${body.delivery.id}/rendered`, payload: {
        rendered_ids: presented, dropped: [], token_budget: 100, rendered_tokens: 10, truncated: false, render_target: 'tool_response' } });
      expect(ack.statusCode, ack.body).toBe(200); expect(ackContract(ack.json())).toBe(true);
      expect((await pool.query('SELECT 1 FROM memory_delivery_acknowledgements WHERE delivery_id=$1', [body.delivery.id])).rowCount).toBe(1);
    }
  });

  it('round trips JSON and legacy client helpers without losing receipts or positional IDs', async () => {
    const client = new PersistioClient(cfg);
    for (const result of [await client.recall('contract', { context }), await client.recallBundle('contract', context)]) {
      expect(result.delivery?.selected_ids.slice().sort()).toEqual([ids.global, ids.direct, ids.graph, ids.candidate].sort());
      await client.acknowledgeRenderedDelivery(result.delivery!.id, { renderedIds: result.delivery!.selected_ids,
        dropped: [], tokenBudget: 100, renderedTokens: 10, truncated: false, renderTarget: 'tool_response' });
    }
  });

  it('returns explicit 400/404 outcomes for malformed, conflicting and foreign-vault ACKs', async () => {
    const response = await app.inject({method:'POST',url:'/v1/recall',payload:{query:'contract',context}});
    const receipt = response.json().delivery;
    const body = { rendered_ids: receipt.selected_ids, dropped: [], token_budget: 100, rendered_tokens: 10, truncated: false, render_target: 'tool_response' };
    for (const invalid of [{}, {...body, rendered_tokens:101}, {...body, token_budget:2147483648}, {...body, extra:true}]) {
      expect((await app.inject({method:'POST',url:`/v1/recall/${receipt.id}/rendered`,payload:invalid})).statusCode).toBe(400);
    }
    const vault = state.vaultId; state.vaultId = crypto.randomUUID();
    try { expect((await app.inject({method:'POST',url:`/v1/recall/${receipt.id}/rendered`,payload:body})).statusCode).toBe(404); }
    finally { state.vaultId = vault; }
    expect((await app.inject({method:'POST',url:`/v1/recall/${receipt.id}/rendered`,payload:body})).statusCode).toBe(200);
    expect((await app.inject({method:'POST',url:`/v1/recall/${receipt.id}/rendered`,payload:{...body,truncated:true}})).statusCode).toBe(400);
  });

  it('publishes terminal metrics only after commit and cannot turn publisher failure into persistence failure',async()=>{
    const receipt=(await app.inject({method:'POST',url:'/v1/recall',payload:{query:'contract',context}})).json().delivery;
    const payload={rendered_ids:receipt.selected_ids,dropped:[],token_budget:100,rendered_tokens:10,truncated:false,render_target:'tool_response'};
    const send=()=>app.inject({method:'POST',url:`/v1/recall/${receipt.id}/rendered`,payload});
    try{
      state.publications=[];state.failCommit=true;
      expect((await send()).statusCode).toBe(500);
      expect(state.publications.filter(p=>p.stage==='rendered'||p.stage==='dropped')).toEqual([]);
      state.failCommit=false;state.throwMetrics=true;state.publications=[];
      expect((await send()).statusCode).toBe(200);
      const terminal=state.publications.filter(p=>p.stage==='rendered'||p.stage==='dropped');
      expect(terminal.every(p=>p.committed)).toBe(true);expect(terminal.reduce((n,p)=>n+p.value,0)).toBe(receipt.selected_count);
      state.publications=[];expect((await send()).statusCode).toBe(200);
      expect(state.publications.filter(p=>p.stage==='rendered'||p.stage==='dropped').reduce((n,p)=>n+p.value,0)).toBe(0);
      expect((await app.inject({method:'POST',url:`/v1/recall/${receipt.id}/rendered`,payload:{}})).statusCode).toBe(400);
    }finally{state.failCommit=false;state.throwMetrics=false;}
  });

  it('keeps request flags and normalized scope identical across server/client', async () => {
    const client = new PersistioClient(resolveConfig({ baseURL: cfg.baseURL, apiKey: 'test', recall: { maxResults: 8 } }));
    const bundle = await client.recallStructuredBundle('contract', { ...context, project_id: ' persistio ' });
    expect(recallBundleMemories(bundle).map(m => m.id)).toEqual([ids.direct]);
    const scheduled = await new PersistioClient(cfg).recallStructuredBundle('contract', { ...context, trigger_type: 'scheduled' });
    expect(recallBundleMemories(scheduled).some(m => m.id === ids.global)).toBe(false);
  });

  it('preserves explicitly enabled legacy global rules as proposed data, never approval', async () => {
    await pool.query("UPDATE memories SET authority_state = 'proposed', authority_version = 3, approved_by = NULL, approved_at = NULL, approval_source = NULL WHERE id = $1", [ids.global]);
    await pool.query(`INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state, new_version, actor_type, source, reason, snapshot)
      VALUES ($1, $2, 'migration', 'proposed', 3, 'system', 'migration', 'legacy contract', '{"type":"user_rule","scope":"global","status":"active","archived_at":null}')`, [state.vaultId, ids.global]);
    state.policy = 'legacy';
    try {
      const bundle = await new PersistioClient(cfg).recallStructuredBundle('contract', context);
      expect(bundle.sections.approved_preferences_and_rules).toEqual([]);
      expect(bundle.sections.historical_facts.find(m => m.id === ids.global)?.authority.state).toBe('proposed');
      state.policy = 'approved_only';
      expect(recallBundleMemories(await new PersistioClient(cfg).recallStructuredBundle('contract', context)).some(m => m.id === ids.global)).toBe(false);
    } finally { state.policy = 'approved_only'; }
  });
});
