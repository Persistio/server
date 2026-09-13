import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({ vector: [] as number[], embed: vi.fn(), unwrap: vi.fn() }));
vi.mock('node:worker_threads', async original => ({ ...(await original() as typeof import('node:worker_threads')), parentPort: null }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed: state.embed }) }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = state.unwrap; } }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('actual curation worker lifecycle (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  const queues: string[] = [];
  let processBatch: typeof import('./curation-worker').processBatch;
  let service: typeof import('../services/curator').CuratorService;
  let config: ReturnType<typeof import('../config').getConfig>;
  let closePool: () => Promise<void>;
  let completion: ReturnType<typeof vi.spyOn>;

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    process.env.CURATOR_API_KEY = 'local-test-model';
    const db = await import('../db/client');
    closePool = db.closePool;
    await db.runMigrations();
    config = (await import('../config')).getConfig();
    config.ENCRYPTION_ENABLED = false;
    config.CURATION_BATCH_SIZE = 1;
    config.CONTRADICTION_SCAN_ENABLED = true;
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    const dimensions = Number(/\((\d+)\)/.exec(type)![1]);
    state.vector = [1, ...Array(dimensions - 1).fill(0)];
    state.embed.mockImplementation(async () => state.vector);
    service = (await import('../services/curator')).CuratorService;
    ({ processBatch } = await import('./curation-worker'));
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    config.CURATION_BATCH_SIZE = 1;
    config.ENCRYPTION_ENABLED = false;
    state.embed.mockReset().mockImplementation(async () => state.vector);
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id = ANY($1::uuid[])', [queues]);
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [vaults]);
    vaults.length = 0;
    queues.length = 0;
  });
  afterAll(async () => { await pool.end(); await closePool?.(); });

  async function fixture(count: number, cap = 12000, activeCount = 0, long = false) {
    const vault = crypto.randomUUID(), segment = crypto.randomUUID(), queue = crypto.randomUUID();
    vaults.push(vault); queues.push(queue);
    await pool.query(`INSERT INTO vaults (id,name,api_key_hash,plan_id,rate_limit_override)
      VALUES ($1,'curation-worker-regression',$2,'unlimited',$3::jsonb)`, [vault, crypto.randomUUID(),
      JSON.stringify({ curator_input_tokens_per_call: cap, curator_candidates_per_call: 40, curator_active_memories_per_call: 80 })]);
    await pool.query(`INSERT INTO segments (id,vault_id,session_id,project_id,chunk_ids,context)
      VALUES ($1,$2,'curation-worker','project','{}','User supplied facts for review.')`, [segment, vault]);
    const candidates: string[] = [], active: string[] = [];
    for (const [status, ids, n] of [['target', candidates, count], ['context', active, activeCount]] as const) {
      for (let i = 0; i < n; i++) {
        const id = crypto.randomUUID(); ids.push(id);
        await pool.query(`INSERT INTO memories (id,vault_id,data,subject,hash,scope,scope_key,status,source_segment_id,
          type,confidence,salience,embedding) VALUES ($1,$2,$3,$4,$5,'project','project',$6,$7,'system_fact',0.9,0.8,$8::vector)`,
        [id, vault, long ? `Fact ${i}. `.repeat(150) : `Fact ${status} ${i}.`, `topic-${i}`, crypto.randomUUID(), 'active',
          status === 'target' ? segment : null, long ? null : JSON.stringify(state.vector)]);
      }
    }
    await pool.query('INSERT INTO curation_queue (id,vault_id,segment_id,work_key) VALUES ($1,$2,$3,$4)', [queue, vault, segment,crypto.randomUUID()]);
    await pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) SELECT $1,vault_id,id,revision FROM memories WHERE id=ANY($2::uuid[])',[queue,candidates]);
    return { vault, segment, queue, candidates, active };
  }
  const empty = () => ({ schema_version:'curation-plan.v2',keep:[],update:[],consolidate:[],archive:[],edges:[],scope_changes:[] } as any);
  const memory = (statement='Refined durable fact',subject='Refined topic') => ({statement,subject,type:'system_fact',
    confidence:0.9,salience:0.8,sensitivity:'low',polarity:'neutral',volatility:'low',valid_from:null,valid_until:null,evidence:'Supported by reviewed memory'});
  const keep = (ids:string[]) => ({...empty(),keep:ids.map(id=>({id,reason:'Already useful'}))});
  function model(plan:(c:string[],m:string[])=>unknown|Promise<unknown>) {
    completion=vi.spyOn(service.prototype as any,'createChatCompletion').mockImplementation(async (request:any,_vault:any,accounting:any)=>{
      await accounting.beforeRequest();
      const parts=request.messages[1].content;
      const ids=(index:number)=>JSON.parse(parts[index].text).memories.map((m:any)=>m.id);
      const result=await plan(ids(0),ids(1));
      await accounting.returnedUsage({promptTokens:100,completionTokens:50,totalTokens:150});
      return {usage:{prompt_tokens:100,completion_tokens:50,total_tokens:150},choices:[{finish_reason:'stop',message:{content:JSON.stringify(result)}}]};
    });
  }
  const refine=(ids:string[])=>({...empty(),update:ids.map(id=>({id,memory:memory(),source_refs:[id],reason:'Refine supported detail'}))});
  it.each([100,2000,11999])('does not claim unsupported input capacity %s or spend model usage', async cap => {
    const f = await fixture(1, cap);
    model(() => { throw new Error('Must not call the model'); });
    await processBatch();
    expect(completion).not.toHaveBeenCalled();
    expect((await pool.query('SELECT retry_count,last_error,available_at > now() AS deferred FROM curation_queue WHERE id=$1', [f.queue])).rows[0])
      .toMatchObject({ retry_count: 0, deferred: false, last_error: null });
    expect((await pool.query('SELECT status FROM memories WHERE id=$1',[f.candidates[0]])).rows[0].status).toBe('active');
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
  });
  it('finishes empty work and dead-letters exhausted work through production fences', async () => {
    const emptyJob = await fixture(0), exhausted = await fixture(1);
    await pool.query('UPDATE curation_queue SET retry_count=100 WHERE id=$1', [exhausted.queue]);
    config.CURATION_BATCH_SIZE = 2;
    model(() => { throw new Error('No model call expected'); });
    await processBatch();
    expect(completion).not.toHaveBeenCalled();
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=ANY($1::uuid[])', [[emptyJob.queue, exhausted.queue]])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM curation_dead_letter WHERE vault_id=$1', [exhausted.vault])).rowCount).toBe(1);
    expect((await pool.query('SELECT action_key FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[]) ORDER BY action_key', [[emptyJob.queue, exhausted.queue]])).rows)
      .toEqual([{ action_key: 'dead-letter' }, { action_key: 'empty-complete' }]);
  });

  it.each(['budget', 'circuit'] as const)('atomically defers %s without retrying and refuses a stale deferral', async reason => {
    const { AiBudgetDeferredError } = await import('../services/usage');
    const { CircuitBreakerOpenError } = await import('../services/ai-resilience');
    const f = await fixture(1);
    model(() => { throw reason === 'budget' ? new AiBudgetDeferredError('curation', new Date(Date.now()+60_000), 60_000) : new CircuitBreakerOpenError('curator', 60_000); });
    await processBatch();
    expect((await pool.query('SELECT retry_count,claim_token,last_error FROM curation_queue WHERE id=$1', [f.queue])).rows[0])
      .toMatchObject({ retry_count: 0, claim_token: null, last_error: expect.any(String) });
    expect((await pool.query('SELECT last_curator_defer_reason FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].last_curator_defer_reason).toBeTruthy();
    await pool.query('UPDATE curation_queue SET available_at=now() WHERE id=$1', [f.queue]);
    await pool.query('UPDATE vault_curation_state SET next_curator_run_at=NULL,last_curator_defer_reason=NULL WHERE vault_id=$1', [f.vault]);
    model(async () => {
      await pool.query("UPDATE curation_queue SET lease_expires_at=now()-interval '1 second' WHERE id=$1", [f.queue]);
      throw new AiBudgetDeferredError('curation', new Date(Date.now()+60_000), 60_000);
    });
    await processBatch();
    expect((await pool.query('SELECT last_curator_defer_reason FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].last_curator_defer_reason).toBeNull();
    expect((await pool.query('SELECT retry_count FROM curation_queue WHERE id=$1', [f.queue])).rows[0].retry_count).toBe(0);
  });
  it('rolls back deferral usage/schedule/release together and stops every heartbeat on batch abort', async () => {
    const f = await fixture(1); await fixture(1); config.CURATION_BATCH_SIZE = 2;
    const { AiBudgetDeferredError } = await import('../services/usage');
    model(() => { throw new AiBudgetDeferredError('curation', new Date(Date.now()+60_000), 60_000); });
    // Applies to either claimed row: first scheduling write fails after its usage insert.
    await pool.query(`CREATE FUNCTION pr370_fail_deferral() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.last_curator_defer_reason IS NOT NULL THEN RAISE EXCEPTION 'deferral schedule failed'; END IF; RETURN NEW; END $$`);
    await pool.query('CREATE TRIGGER pr370_fail_deferral BEFORE UPDATE ON vault_curation_state FOR EACH ROW EXECUTE FUNCTION pr370_fail_deferral()');
    const starts = vi.spyOn(globalThis, 'setInterval'), stops = vi.spyOn(globalThis, 'clearInterval');
    try { await expect(processBatch()).rejects.toThrow('deferral schedule failed'); }
    finally { await pool.query('DROP TRIGGER pr370_fail_deferral ON vault_curation_state'); await pool.query('DROP FUNCTION pr370_fail_deferral()'); }
    const handles = starts.mock.results.filter(result => result.type === 'return').map(result => result.value);
    expect(handles).toHaveLength(2);
    for (const handle of handles) expect(stops).toHaveBeenCalledWith(handle);
    // A paid request attempt remains accounted even when scheduling its retry rolls back.
    expect((await pool.query('SELECT sum(curator_requests)::int AS n FROM vault_usage WHERE vault_id=ANY($1::uuid[])',[vaults])).rows[0].n).toBe(1);
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=$1 AND claim_token IS NOT NULL AND retry_count=0', [f.queue])).rowCount).toBe(1);
    expect((await pool.query('SELECT last_curator_defer_reason FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].last_curator_defer_reason).toBeNull();
  });
  it('claims multiple vaults concurrently without duplicating ownership or deadlocking', async () => {
    await fixture(1); await fixture(1); await fixture(1); await fixture(1);
    const { claimEligibleCurationJobs } = await import('../services/curation-capacity');
    const results = await Promise.all([claimEligibleCurationJobs(2, 'claim-a'), claimEligibleCurationJobs(2, 'claim-b')]);
    const ids = results.flat().map(row => row.queue_id);
    expect(ids.length).toBeGreaterThan(0);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('processes only complete records within the bounded prompt and retains unreviewed targets',async()=>{
    const f=await fixture(20,12000,20,true);let reviewed=0;
    model((targets)=>{reviewed=targets.length;return keep(targets);});
    await processBatch();
    expect(completion).toHaveBeenCalledOnce();
    expect(reviewed).toBeGreaterThan(0);expect(reviewed).toBeLessThan(20);
    expect((await pool.query('SELECT count(*)::int AS n FROM memories WHERE vault_id=$1 AND status=$2',[f.vault,'active'])).rows[0].n).toBe(40);
    expect((await pool.query('SELECT count(*)::int AS n FROM curation_queue_items WHERE queue_id=$1',[f.queue])).rows[0].n).toBe(20-reviewed);
    expect((await pool.query('SELECT curator_candidates_processed,curator_requests FROM vault_usage WHERE vault_id=$1',[f.vault])).rows[0])
      .toEqual({curator_candidates_processed:reviewed,curator_requests:1});
    await processBatch();expect(completion).toHaveBeenCalledOnce();
  });
  it.each(['embedding-failure','queue-delete-failure'])('preserves active baseline after %s while retaining paid usage',async failure=>{
    const f=await fixture(1);model(refine);
    if(failure==='embedding-failure')state.embed.mockRejectedValue(new Error('Synthetic embedding failure'));
    else {
      await pool.query("CREATE FUNCTION restoration_fail_queue_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Synthetic finish failure'; END $$");
      await pool.query('CREATE TRIGGER restoration_fail_queue_delete BEFORE DELETE ON curation_queue FOR EACH ROW EXECUTE FUNCTION restoration_fail_queue_delete()');
    }
    try {await processBatch();}
    finally {if(failure==='queue-delete-failure'){await pool.query('DROP TRIGGER restoration_fail_queue_delete ON curation_queue');await pool.query('DROP FUNCTION restoration_fail_queue_delete()');}}
    expect((await pool.query('SELECT data,status,revision::text FROM memories WHERE id=$1',[f.candidates[0]])).rows[0])
      .toEqual({data:'Fact target 0.',status:'active',revision:'1'});
    expect((await pool.query('SELECT 1 FROM curation_action_log WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
    expect((await pool.query("SELECT 1 FROM worker_action_receipts WHERE queue_id=$1 AND action_key LIKE 'apply-actions:%'",[f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_requests,curator_candidates_processed FROM vault_usage WHERE vault_id=$1',[f.vault])).rows[0])
      .toEqual({curator_requests:1,curator_candidates_processed:0});
  });
  it('holds no mutation locks during embedding preparation',async()=>{
    const f=await fixture(1);model(refine);
    state.embed.mockImplementation(async()=>{
      const probe=await pool.connect();
      try {
        await probe.query('BEGIN');
        for(const [table,key,id] of [['curation_queue','id',f.queue],['vault_curation_state','vault_id',f.vault],['vaults','id',f.vault],['memories','vault_id',f.vault]]) {
          await probe.query('SELECT * FROM '+table+' WHERE '+key+'=$1 FOR UPDATE NOWAIT',[id]);
        }
      } finally {await probe.query('ROLLBACK');probe.release();}
      return state.vector;
    });
    await processBatch();expect(state.embed).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1',[f.vault])).rows[0].validation_status).toBe('applied');
  });
  it.each(['target','context','vault-lease'])('refuses the entire graph after %s changes during embedding',async change=>{
    const f=await fixture(1,12000,1);model(refine);
    state.embed.mockImplementation(async()=>{
      if(change==='vault-lease')await pool.query("UPDATE vault_curation_state SET curator_claimed_until=now()-interval '1 second' WHERE vault_id=$1",[f.vault]);
      else await pool.query("UPDATE memories SET data='Independent edit' WHERE id=$1",[change==='target'?f.candidates[0]:f.active[0]]);
      return state.vector;
    });
    await processBatch();
    expect((await pool.query("SELECT 1 FROM memories WHERE vault_id=$1 AND data='Refined durable fact'",[f.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_requests,curator_candidates_processed FROM vault_usage WHERE vault_id=$1',[f.vault])).rows[0])
      .toEqual({curator_requests:1,curator_candidates_processed:0});
  });
  it('does not repeat committed application after its COMMIT response is lost',async()=>{
    const f=await fixture(1);model(refine);
    const db=await import('../db/client'),transaction=db.withTransaction;
    let lost=false;
    vi.spyOn(db,'withTransaction').mockImplementation(async work=>{
      let applied=false;
      const result=await transaction(async client=>{
        const original=client.query.bind(client);
        const spy=vi.spyOn(client,'query').mockImplementation((...args:any[])=>{
          if(typeof args[0]==='string'&&args[0].includes("validation_status='applied'"))applied=true;
          return (original as any)(...args);
        });
        try{return await work(client);}finally{spy.mockRestore();}
      });
      if(applied&&!lost){lost=true;throw new Error('Synthetic lost COMMIT response');}
      return result;
    });
    await processBatch();expect(lost).toBe(true);
    expect((await pool.query('SELECT id FROM curation_queue WHERE id=$1',[f.queue])).rowCount).toBe(0);
    await processBatch();expect(completion).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT curator_requests,curator_candidates_processed FROM vault_usage WHERE vault_id=$1',[f.vault])).rows[0])
      .toEqual({curator_requests:1,curator_candidates_processed:1});
    expect((await pool.query('SELECT data FROM memories WHERE id=$1',[f.candidates[0]])).rows[0].data).toBe('Refined durable fact');
  });
  it('keeps committed curation completed if count publication fails',async()=>{
    const f=await fixture(1);model(ids=>({...empty(),archive:ids.map(id=>({id,reason:'No future-use information',basis:'no_durable_value'}))}));
    vi.spyOn(await import('../services/usage'),'recordMemoryCountDelta').mockImplementation(()=>{throw new Error('Synthetic publisher failure');});
    await processBatch();await processBatch();expect(completion).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT id FROM curation_queue WHERE id=$1',[f.queue])).rowCount).toBe(0);
  });
  it('processes independent improvement groups under one vault claim',async()=>{
    const f=await fixture(1);config.CURATION_BATCH_SIZE=2;
    const queue=crypto.randomUUID(),id=crypto.randomUUID();queues.push(queue);
    await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,scope,scope_key,status,type) VALUES ($1,$2,'Another durable fact','other',$3,'project','project','active','system_fact')",[id,f.vault,crypto.randomUUID()]);
    await pool.query('INSERT INTO curation_queue(id,vault_id,work_key) VALUES ($1,$2,$3)',[queue,f.vault,crypto.randomUUID()]);
    await pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) SELECT $1,vault_id,id,revision FROM memories WHERE id=$2',[queue,id]);
    model(keep);await processBatch();
    expect(completion).toHaveBeenCalledTimes(2);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_runs,curator_requests FROM vault_usage WHERE vault_id=$1',[f.vault])).rows[0])
      .toEqual({curator_runs:1,curator_requests:2});
  });
  it('applies encrypted curation with no KMS call under locks and no plaintext audit payload', async () => {
    const f = await fixture(1);
    const key = Buffer.alloc(32, 11);
    const crypt = await import('../services/crypto');
    config.KEY_PROVIDER = 'gcp_kms'; config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    config.ENCRYPTION_ENABLED = true;
    state.unwrap.mockReset().mockImplementation(async () => {
      const probe = await pool.connect();
      try {
        await probe.query('BEGIN');
        await probe.query('SELECT * FROM curation_queue WHERE id=$1 FOR UPDATE NOWAIT', [f.queue]);
        await probe.query('SELECT * FROM vault_curation_state WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
        await probe.query('SELECT * FROM memories WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
      } finally { await probe.query('ROLLBACK'); probe.release(); }
      return [{ plaintext: key }];
    });
    await crypt.initCryptoClient();
    await pool.query('UPDATE vaults SET encrypted_dek=$2,vault_encryption_enabled=true WHERE id=$1', [f.vault, Buffer.from('wrapped-fixture').toString('base64')]);
    await pool.query('UPDATE segments SET context=$2 WHERE id=$1', [f.segment, crypt.encryptField('User supplied facts for review.', key)]);
    await pool.query("UPDATE memories SET data=$2,subject='',subject_encrypted=$3,subject_hmac=$4 WHERE id=$1", [f.candidates[0],
      crypt.encryptField('Fact candidate 0.', key), crypt.encryptField('topic-0', key), crypt.computeSubjectHmac('topic-0', key)]);
    await pool.query('UPDATE curation_queue_items SET revision=(SELECT revision FROM memories WHERE id=$1) WHERE queue_id=$2',[f.candidates[0],f.queue]);
    model(ids=>({...empty(),update:ids.map(id=>({id,memory:memory('Created private topic','private topic'),source_refs:[id],reason:'Supported refinement'}))}));
    await processBatch();
    const audit = (await pool.query('SELECT validation_status,raw_response,before_state,after_state FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(audit, JSON.stringify((await pool.query('SELECT last_error FROM curation_queue WHERE id=$1',[f.queue])).rows)).toBeDefined();
    expect(audit.validation_status).toBe('applied');
    expect(audit.raw_response).toHaveProperty('encrypted'); expect(audit.before_state).toHaveProperty('encrypted'); expect(audit.after_state).toHaveProperty('encrypted');
    expect(JSON.stringify(audit)).not.toContain('private topic');
    const storedMemory = (await pool.query("SELECT data,subject_encrypted FROM memories WHERE vault_id=$1 AND status='active'", [f.vault])).rows[0];
    expect(crypt.decryptField(storedMemory.data, key)).toBe('Created private topic');
    expect(crypt.decryptField(storedMemory.subject_encrypted, key)).toBe('private topic');
    const logs = (await pool.query('SELECT subject,new_value,raw_curator_response FROM curation_action_log WHERE vault_id=$1', [f.vault])).rows;
    expect(JSON.stringify(logs)).not.toContain('private topic');
    expect(state.unwrap).toHaveBeenCalledTimes(2); // Read preparation + exact wrapped-key apply preparation; none under locks.
  });

});
