import crypto from 'node:crypto';
import Fastify from 'fastify';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({ pool: null as any, port: null as any, vault: null as any,
  blobs: new Map<string, string>(), dimensions: 1536, extract: vi.fn(), context: vi.fn(async () => null) }));
vi.mock('../../db/client', () => ({
  query: (sql: string, args: unknown[] = []) => sql.includes('WITH claimed AS') && args.length === 2
    ? Promise.resolve({ rows: [], rowCount: 0 }) : state.pool.query(sql, args),
  withTransaction: async (callback: (client: any) => unknown) => {
    const client = await state.pool.connect();
    try { await client.query('BEGIN'); const result = await callback(client); await client.query('COMMIT'); return result; }
    catch (error) { await client.query('ROLLBACK'); throw error; } finally { client.release(); }
  },
  closePool: async () => {}
}));
vi.mock('node:worker_threads', async original => ({ ...await original<typeof import('node:worker_threads')>(),
  parentPort: null
}));
vi.mock('../../config', async original => {
  const actual = await original<typeof import('../../config')>();
  return { ...actual, getConfig: () => ({ ...actual.getConfig(), ENCRYPTION_ENABLED: false, CURATOR_AUTO_RUN: false,
    EXTRACTION_INTERVAL_MS: 600_000, EMBEDDING_DIMENSIONS: state.dimensions }) };
});
vi.mock('../../middleware/auth', () => ({ requireVaultWriteAuth: async (request: any) => { request.vault = state.vault; } }));
vi.mock('../raw-chunk-storage', () => ({
  createRawChunkBlobKey: (_vault: string, _session: string, id: string) => id,
  getRawChunkStorage: () => ({ store: 'local',
    put: async (key: string, value: string) => { state.blobs.set(key, value); return { blobStore: 'local', blobKey: key }; },
    get: async (key: string) => { if (!state.blobs.has(key)) throw new Error('Missing test blob'); return state.blobs.get(key); },
    delete: async (key: string) => { state.blobs.delete(key); }
  })
}));
vi.mock('../embedder', () => ({
  OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT: 8192,
  estimateEmbeddingTokens: (text: string) => Math.ceil(text.length / 3),
  getEmbedder: () => ({ embedBatch: async (texts: string[]) => texts.map(() => [1, ...Array(state.dimensions - 1).fill(0)]) })
}));
vi.mock('../extractor', () => ({ ExtractorService: class {
  extractSessionContext = state.context;
  extractFacts = state.extract;
  arbitrateSubject = async () => 'use_existing';
} }));
vi.mock('../contradiction-activation', async original => ({ ...await original<typeof import('../contradiction-activation')>(), drainDueContradictionActivations: async () => 0 }));
vi.mock('../customer-metrics', () => ({ initCustomerMetrics: async () => {}, shutdownCustomerMetrics: async () => {}, recordCustomerMetric: () => {} }));
// Background global cleanup is not part of this vault-scoped replay test.
vi.mock('../staleness', () => ({ archiveStaleMemories: async () => 0 }));


const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('prepared replay through actual ingestion and extraction (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl});let vaultId:string;
  let app:ReturnType<typeof Fastify>,processBatch:typeof import('../../daemon/extraction-worker').processBatch;
  let prepare:(rows:unknown[],options:unknown)=>any[];
  beforeAll(async()=>{
    state.pool=pool;
    const type=await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memory_embeddings'::regclass AND attname='embedding'");
    state.dimensions=Number(/vector\((\d+)\)/.exec(type.rows[0].type)![1]);
    ({prepareReplayDataset:prepare}=await import(new URL('../../../../../scripts/lib/replay-dataset.mjs',import.meta.url).href));
    app=Fastify();await(await import('../../routes/ingest')).registerIngestRoutes(app);
    ({processBatch}=await import('../../daemon/extraction-worker'));
  });
  beforeEach(async()=>{
    vaultId=crypto.randomUUID();state.blobs.clear();state.extract.mockReset();state.context.mockClear();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES ($1,'replay-restoration',$2,'unlimited')",[vaultId,crypto.randomUUID()]);
    state.vault={id:vaultId,name:'test',plan_id:'unlimited',purpose:null,settings:{},status:'active',encrypted_dek:null,vault_encryption_enabled:false};
  });
  afterEach(async()=>{
    await pool.query('DELETE FROM extraction_queue WHERE vault_id=$1',[vaultId]);
    await pool.query('DELETE FROM vaults WHERE id=$1',[vaultId]);
  });
  afterAll(async()=>{await app?.close();await pool.end();});
  const human={actor_type:'human',authorship:'original',trigger_type:'direct',artifact_type:'message',cadence:'one_off',
    payload_author:{actor_type:'human',authorship:'original',is_user:true}};
  const agent={...human,actor_type:'agent',authorship:'generated',payload_author:{actor_type:'assistant',authorship:'generated',is_user:false}};
  const fact=(ref:string,type='system_fact')=>({fact:type==='system_fact'?'The project runtime is Node.js 22.':'The user prefers concise technical explanations.',
    subject:type==='system_fact'?'Project runtime':'User explanation style',type,scope:'session',scope_basis:'Explicit session source',
    score:9,sensitivity:'low',salience:0.8,polarity:'neutral',volatility:'low',evidence:'Source statement',
    source_refs:[ref],valid_from:null,valid_until:null});
  const memories=()=>pool.query('SELECT type,status,scope FROM memories WHERE vault_id=$1 ORDER BY type',[vaultId]);
  async function ingest(provenance:unknown=human,url='/v1/ingest/bulk',context:any={trigger_type:'direct'},content='Durable source information'){
    const response=await app.inject({method:'POST',url,payload:{session_id:crypto.randomUUID(),context,chunks:[{
      role:'user',content,timestamp:'2026-06-01T00:00:00Z',...(provenance?{provenance}:{})}]}});
    expect(response.statusCode,response.body).toBe(202);return response.json();
  }
  const current=(conversation:string)=>JSON.parse(conversation).sources.find((s:any)=>s.current);
  it('preserves transported-tail authorship regardless of arrival order, without treating it as human intent',async()=>{
    const timestamp='2026-06-01T00:00:00Z';
    const source=(segment_id:string,id:string,content:string,session_id='transport')=>({segment_id,session_id,created_at:timestamp,
      chunks:[{id,role:'user',content,created_at:timestamp,event_id:'shared-event'}]});
    const plan=prepare([
      source('header','h','[Inter-session message] sourceSession=agent:main:subagent:sender sourceChannel=internal sourceTool=sessions_send isUser=false\nGenerated payload'),
      source('tail','t','Cancel this task; do not turn this into a standing rule.'),
      source('ordinary','o','The runtime is Node.js 22.','ordinary')
    ],{datasetSha256:'a'.repeat(64),importJobId:'job'});
    for(const segment of [plan[1],plan[0],plan[2]]){
      const payload={session_id:segment.session_id,context:{trigger_type:'backfill'},chunks:segment.chunks};
      const accepted=await app.inject({method:'POST',url:'/v1/ingest/bulk',payload});
      expect(accepted.statusCode,accepted.body).toBe(202);
      expect((await app.inject({method:'POST',url:'/v1/ingest/bulk',payload})).json()).toMatchObject({inserted:0,replayed:1});
    }
    const stored=await pool.query('SELECT session_id,provenance FROM raw_chunks WHERE vault_id=$1',[vaultId]);
    expect(stored.rows.filter(r=>r.session_id==='transport').every(r=>r.provenance.payload_author.is_user===false)).toBe(true);
    state.extract.mockImplementation(async(conversation:string)=>{
      const source=current(conversation);
      if(source.provenance?.payload_author?.is_user===false){expect(source.human_intent_source).toBe(false);return [];}
      return [fact(source.ref)];
    });
    await processBatch(vaultId);
    expect((await memories()).rows).toEqual([{type:'system_fact',status:'active',scope:'session'}]);
    expect((await pool.query("SELECT 1 FROM jobs WHERE vault_id=$1 AND status<>'completed'",[vaultId])).rowCount).toBe(0);
  });
  it.each(['/v1/ingest','/v1/ingest/bulk'])('rejects malformed source structure at %s before storing or invoking providers',async url=>{
    for(const key of ['payload_author','transport','import']){
      const response=await app.inject({method:'POST',url,payload:{session_id:'bad',chunks:[{role:'user',content:'Claimed human source',
        timestamp:'2026-06-01T00:00:00Z',provenance:{...human,[key]:{}}}]}});
      expect(response.statusCode,response.body).toBe(400);
    }
    expect((await pool.query('SELECT 1 FROM raw_chunks WHERE vault_id=$1',[vaultId])).rowCount).toBe(0);
    expect(state.extract).not.toHaveBeenCalled();
  });
  it.each(['direct','scheduled','event','backfill'])('keeps source-supported human preferences usable for %s capture without review states',async trigger_type=>{
    await ingest(human,'/v1/ingest',{trigger_type});
    state.extract.mockImplementation(async(conversation:string)=>[fact(current(conversation).ref),fact(current(conversation).ref,'user_preference')]);
    await processBatch(vaultId);
    expect((await memories()).rows).toEqual([{type:'system_fact',status:'active',scope:'session'},{type:'user_preference',status:'active',scope:'session'}]);
  });
  it.each([agent,{...human,payload_author:{actor_type:'agent',authorship:'original',is_user:true}}])
  ('does not let role=user override nonhuman payload evidence: %j',async provenance=>{
    await ingest(provenance);
    state.extract.mockImplementation(async(conversation:string)=>[fact(current(conversation).ref,'user_rule')]);
    await processBatch(vaultId);
    expect((await memories()).rows).toEqual([]);
    expect((await pool.query('SELECT retry_count FROM extraction_queue WHERE vault_id=$1',[vaultId])).rows).toEqual([]);
    expect((await pool.query('SELECT processed FROM raw_chunks WHERE vault_id=$1',[vaultId])).rows).toEqual([{processed:true}]);
    expect((await pool.query("SELECT 1 FROM jobs WHERE vault_id=$1 AND status<>'completed'",[vaultId])).rows).toEqual([]);
    // Completion is terminal, not a retry loop. A new capture from the same
    // nonhuman author can still supply useful facts; the author is not quarantined.
    await processBatch(vaultId);expect(state.extract).toHaveBeenCalledOnce();
    await ingest(provenance,'/v1/ingest/bulk',{trigger_type:'direct'},'The project runtime is Node.js 22.');
    state.extract.mockImplementation(async(conversation:string)=>[fact(current(conversation).ref)]);
    await processBatch(vaultId);
    expect((await memories()).rows).toEqual([{type:'system_fact',status:'active',scope:'session'}]);
  });
  it('rejects a malformed whole model result, then permits a valid retry without partial memory writes',async()=>{
    const {ExtractorService}=await vi.importActual<typeof import('../extractor')>('../extractor');
    const parser=new ExtractorService();
    const provider=vi.spyOn(parser as any,'createChatCompletion').mockResolvedValue({choices:[{finish_reason:'stop',message:{content:JSON.stringify({facts:[
      fact('S1'),{...fact('S1'),valid_from:'2026-06-02',valid_until:'2026-06-01'}
    ]})}}]});
    await ingest(human);
    state.extract.mockImplementation((conversation:string)=>parser.extractFacts(conversation));
    try{
      await processBatch(vaultId);
      expect((await memories()).rows).toEqual([]);
      expect((await pool.query('SELECT retry_count FROM extraction_queue WHERE vault_id=$1',[vaultId])).rows).toEqual([{retry_count:1}]);
      provider.mockResolvedValue({choices:[{finish_reason:'stop',message:{content:JSON.stringify({facts:[fact('S1')]})}}]});
      await pool.query('UPDATE extraction_queue SET available_at=now() WHERE vault_id=$1',[vaultId]);
      await processBatch(vaultId);
      expect((await memories()).rows,JSON.stringify((await pool.query('SELECT last_error FROM extraction_queue WHERE vault_id=$1',[vaultId])).rows)).toEqual([{type:'system_fact',status:'active',scope:'session'}]);
    }finally{provider.mockRestore();}
  });
});
