import crypto from 'node:crypto';
import { Pool } from 'pg';
import { beforeAll,afterAll,afterEach,describe,it,expect,vi } from 'vitest';
import type { CuratorResult } from '../services/curator-contract';

const state=vi.hoisted(()=>({embed:vi.fn(),read:vi.fn()}));
vi.mock('node:worker_threads',async original=>({...await original() as any,parentPort:null}));
vi.mock('../services/embedder',()=>({getEmbedder:()=>({embed:state.embed})}));
vi.mock('../services/raw-chunk-storage',()=>({getRawChunkStorage:()=>({store:'local',get:state.read})}));
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('active-memory curation worker (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl});
  const vaults:string[]=[],queues:string[]=[];
  let db:typeof import('../db/client');
  let worker:typeof import('./curation-worker');
  let CuratorService:typeof import('../services/curator').CuratorService;
  let config:ReturnType<typeof import('../config').getConfig>;
  let vector:number[];
  beforeAll(async()=>{
    process.env.DATABASE_URL=databaseUrl;
    db=await import('../db/client');await db.runMigrations();
    config=(await import('../config')).getConfig();config.ENCRYPTION_ENABLED=false;config.CURATION_BATCH_SIZE=1;
    const type=(await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    const dimensions=Number(/\((\d+)\)/.exec(type)![1]);
    vector=[1,...Array(dimensions-1).fill(0)];state.embed.mockResolvedValue(vector);
    ({CuratorService}=await import('../services/curator'));worker=await import('./curation-worker');
  });
  afterEach(async()=>{
    vi.restoreAllMocks();state.embed.mockReset().mockResolvedValue(vector);state.read.mockReset();
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])',[queues]);
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;queues.length=0;
  });
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function fixture(count=2){
    const vault=crypto.randomUUID(),queue=crypto.randomUUID();vaults.push(vault);queues.push(queue);
    await pool.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'active-curation-test',$2,'unlimited')`,[vault,crypto.randomUUID()]);
    const ids:string[]=[];
    for(let i=0;i<count;i++){
      const id=crypto.randomUUID();ids.push(id);
      await pool.query(`INSERT INTO memories(id,vault_id,data,subject,hash,scope,scope_key,status,type,confidence,salience,embedding)
        VALUES($1,$2,$3,'Test topic',$4,'session','session1','active','system_fact',0.9,0.8,$5::vector)`,
        [id,vault,`Durable fact ${i}`,crypto.randomUUID(),JSON.stringify(vector)]);
      await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)',[id,JSON.stringify(vector)]);
    }
    await pool.query('INSERT INTO curation_queue(id,vault_id,work_key) VALUES($1,$2,$3)',[queue,vault,crypto.randomUUID()]);
    await pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) SELECT $1,vault_id,id,revision FROM memories WHERE vault_id=$2',[queue,vault]);
    return{vault,queue,ids};
  }
  const empty=():CuratorResult=>({schema_version:'curation-plan.v2',keep:[],update:[],consolidate:[],archive:[],edges:[],scope_changes:[]});
  const proposed={statement:'Durable consolidated fact',subject:'Test topic',type:'system_fact' as const,confidence:0.9,salience:0.8,
    sensitivity:'low' as const,polarity:'neutral' as const,volatility:'low' as const,valid_from:null,valid_until:null,evidence:'Supporting memories'};
  function model(fn:(targets:any[],context:any[],sources:any[])=>unknown|Promise<unknown>,finish='stop'){
    return vi.spyOn(CuratorService.prototype as any,'createChatCompletion').mockImplementation(async(request:any,_vault:any,accounting:any)=>{
      await accounting.beforeRequest();
      const result=await fn(JSON.parse(request.messages[1].content[0].text).memories,JSON.parse(request.messages[1].content[1].text).memories,
        JSON.parse(request.messages[1].content[2].text).sources);
      await accounting.returnedUsage({promptTokens:100,completionTokens:50,totalTokens:150});
      return{usage:{prompt_tokens:100,completion_tokens:50,total_tokens:150},choices:[{finish_reason:finish,message:{content:JSON.stringify(result)}}]};
    });
  }
  async function usage(vault:string){return(await pool.query('SELECT curator_requests,curator_runs,curator_input_tokens,curator_output_tokens,curator_candidates_processed FROM vault_usage WHERE vault_id=$1',[vault])).rows[0];}
  async function semanticState(vault:string){
    return{
      memories:(await pool.query('SELECT id,data,status,revision::text,source_chunks,evidence,archived_at FROM memories WHERE vault_id=$1 ORDER BY id',[vault])).rows,
      embeddings:(await pool.query('SELECT e.memory_id,md5(e.embedding::text) AS fingerprint FROM memory_embeddings e JOIN memories m ON m.id=e.memory_id WHERE m.vault_id=$1 ORDER BY e.memory_id',[vault])).rows,
      edges:(await pool.query('SELECT id,from_memory_id,to_memory_id,type,confidence,reason FROM memory_edges WHERE vault_id=$1 ORDER BY id',[vault])).rows,
      actions:(await pool.query('SELECT id FROM curation_action_log WHERE vault_id=$1',[vault])).rowCount,
      mutations:(await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[vault])).rowCount
    };
  }
  async function makeFixtureDue(vault:string){
    // Advance only this fixture's scheduler, exercising the real claim path
    // without sleeping or disabling the production scheduling contract.
    await pool.query("UPDATE vault_curation_state SET next_curator_run_at=now()-interval '1 second' WHERE vault_id=$1",[vault]);
  }
  it('keeps active targets available, completes membership and does not self-enqueue',async()=>{
    const f=await fixture();const call=model(targets=>({...empty(),keep:targets.map(m=>({id:m.id,reason:'Already useful'}))}));
    await worker.processBatch();expect(call).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT status,revision::text FROM memories WHERE vault_id=$1',[f.vault])).rows).toEqual([{status:'active',revision:'1'},{status:'active',revision:'1'}]);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
    expect(await usage(f.vault)).toEqual({curator_requests:1,curator_runs:1,curator_input_tokens:100,curator_output_tokens:50,curator_candidates_processed:2});
    await worker.processBatch();expect(call).toHaveBeenCalledOnce();
  });
  it('selects readable optional sources before the cap without gating active memories',async()=>{
    const f=await fixture(1),ids:string[]=[];
    for(let i=0;i<11;i++){
      const id=crypto.randomUUID();ids.push(id);
      await pool.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes)
        VALUES($1::uuid,$2,'session1','user',$3,$1::text,20)`,[id,f.vault,i===0?'local':'gcs']);
    }
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1',[f.ids[0],ids]);
    await pool.query('UPDATE curation_queue_items i SET revision=m.revision FROM memories m WHERE m.id=i.memory_id AND i.queue_id=$1',[f.queue]);
    state.read.mockResolvedValue('Durable source fact');
    const call=model((targets,_context,sources)=>{
      expect(sources).toHaveLength(1);expect(sources[0].content).toBe('Durable source fact');
      return{...empty(),keep:targets.map(t=>({id:t.id,reason:'Already useful'}))};
    });
    await worker.processBatch();expect(call).toHaveBeenCalledOnce();expect(state.read.mock.calls).toEqual([[ids[0]]]);
    expect((await pool.query('SELECT status FROM memories WHERE id=$1',[f.ids[0]])).rows[0].status).toBe('active');
    expect((await pool.query('SELECT id FROM curation_queue WHERE id=$1',[f.queue])).rowCount).toBe(0);
  });
  it('consolidates atomically with replacement embedding, lineage and rewired graph',async()=>{
    const f=await fixture(3);
    // The third memory is reviewed context, not consumed improvement work.
    await pool.query('DELETE FROM curation_queue_items WHERE queue_id=$1 AND memory_id=$2',[f.queue,f.ids[2]]);
    await pool.query(`INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'supports',0.8,'existing evidence')`,[f.vault,f.ids[0],f.ids[2]]);
    model(targets=>({...empty(),consolidate:[{id:'N1',sources:targets.map(t=>t.id),memory:proposed,reason:'Equivalent information'}]}));
    await worker.processBatch();
    const rows=(await pool.query('SELECT id,status,data FROM memories WHERE vault_id=$1 ORDER BY data',[f.vault])).rows;
    const replacement=rows.find(r=>r.data===proposed.statement);expect(replacement?.status).toBe('active');
    expect(rows.filter(r=>r.status==='superseded')).toHaveLength(2);
    expect((await pool.query('SELECT memory_id FROM memory_embeddings WHERE memory_id=$1',[replacement.id])).rowCount).toBe(1);
    expect((await pool.query('SELECT from_memory_id,to_memory_id,reason FROM memory_edges WHERE vault_id=$1',[f.vault])).rows)
      .toEqual([{from_memory_id:replacement.id,to_memory_id:f.ids[2],reason:'existing evidence'}]);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
  });
  it.each(['missing-disposition','truncated','cross-binding','disjoint-dates','disjoint-update','overlapping-distinct-dates'] as const)('rejects %s without hiding useful memories, but accounts paid response',async kind=>{
    const f=await fixture();
    if(kind==='cross-binding')await pool.query("UPDATE memories SET scope_key='other' WHERE id=$1",[f.ids[1]]);
    if(kind==='disjoint-dates'||kind==='disjoint-update'){
      await pool.query("UPDATE memories SET valid_until='2020-12-31' WHERE id=$1",[f.ids[0]]);
      await pool.query("UPDATE memories SET valid_from='2021-01-01' WHERE id=$1",[f.ids[1]]);
    }
    if(kind==='overlapping-distinct-dates')await pool.query("UPDATE memories SET valid_until='2020-12-31' WHERE id=$1",[f.ids[0]]);
    await pool.query('UPDATE curation_queue_items i SET revision=m.revision FROM memories m WHERE m.id=i.memory_id AND i.queue_id=$1',[f.queue]);
    model(targets=>kind==='disjoint-update'?{...empty(),keep:[{id:targets[1].id,reason:'Useful'}],
      update:[{id:targets[0].id,memory:proposed,source_refs:targets.map(t=>t.id),reason:'Combine different dates'}]}:
      ['cross-binding','disjoint-dates','overlapping-distinct-dates'].includes(kind)?{...empty(),consolidate:[{id:'N1',sources:targets.map(t=>t.id),memory:proposed,reason:'Merge'}]}:
      kind==='missing-disposition'?empty():{...empty(),keep:targets.map(t=>({id:t.id,reason:'Useful'}))},kind==='truncated'?'length':'stop');
    await worker.processBatch();
    expect((await pool.query('SELECT id FROM memories WHERE vault_id=$1 AND status=\'active\' AND archived_at IS NULL',[f.vault])).rowCount).toBe(2);
    expect((await pool.query('SELECT retry_count FROM curation_queue WHERE id=$1',[f.queue])).rows[0].retry_count).toBe(1);
    expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_input_tokens:100,curator_output_tokens:50,curator_candidates_processed:0});
  });
  it('rejects a stale complete plan and preserves the separately queued newer revision',async()=>{
    const f=await fixture(1);let changedQueue:string|undefined;
    model(async targets=>{
      await db.withTransaction(async client=>{
        await client.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE',[f.vault]);
        await client.query("UPDATE memories SET data='New independent correction' WHERE id=$1",[f.ids[0]]);
        const {enqueueCurationWork}=await import('../services/curation-work');
        changedQueue=(await enqueueCurationWork(client,{vaultId:f.vault,workKey:crypto.randomUUID(),memoryIds:f.ids}))!;queues.push(changedQueue);
      });
      return{...empty(),update:[{id:targets[0].id,memory:proposed,source_refs:[targets[0].id],reason:'Improve'}]};
    });
    await worker.processBatch();
    expect((await pool.query('SELECT data FROM memories WHERE id=$1',[f.ids[0]])).rows[0].data).toBe('New independent correction');
    expect((await pool.query('SELECT revision::text FROM curation_queue_items WHERE queue_id=$1',[changedQueue])).rows).toEqual([{revision:'2'}]);
    expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_candidates_processed:0});
  });
  it('provider failure leaves active knowledge and records the attempted request without invented tokens',async()=>{
    const f=await fixture(1);model(()=>{throw new Error('Synthetic provider failure');});await worker.processBatch();
    expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_input_tokens:0,curator_output_tokens:0,curator_candidates_processed:0});
    expect((await pool.query('SELECT status FROM memories WHERE id=$1',[f.ids[0]])).rows[0].status).toBe('active');
  });

  it('rejects a partially well-formed plan then applies one complete retry without losing the invalid audit or paid usage',async()=>{
    const f=await fixture();
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'supports',0.8,'Existing evidence')",[f.vault,...f.ids]);
    const before=await semanticState(f.vault);
    let attempts=0;
    const call=model(targets=>++attempts===1?{
      ...empty(),update:[
        {id:targets[0].id,memory:proposed,source_refs:[targets[0].id],reason:'Supported refinement'},
        {id:targets[1].id,memory:'Invalid replacement shape',source_refs:[targets[1].id],reason:'Malformed second action'}
      ]
    }:{...empty(),update:[{id:targets[0].id,memory:proposed,source_refs:[targets[0].id],reason:'Supported refinement'}],
      keep:[{id:targets[1].id,reason:'Already useful'}]});
    await worker.processBatch();
    expect(call).toHaveBeenCalledOnce();
    expect(state.embed).not.toHaveBeenCalled();
    expect(await semanticState(f.vault)).toEqual(before);
    expect((await pool.query('SELECT retry_count,claim_token FROM curation_queue WHERE id=$1',[f.queue])).rows)
      .toEqual([{retry_count:1,claim_token:null}]);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1',[f.vault])).rows)
      .toEqual([{validation_status:'invalid'}]);
    expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_input_tokens:100,curator_output_tokens:50,curator_candidates_processed:0});

    await makeFixtureDue(f.vault);await worker.processBatch();
    expect(call).toHaveBeenCalledTimes(2);
    const recovered=await semanticState(f.vault);
    expect(recovered.memories).toHaveLength(2);
    expect(recovered.memories.filter(memory=>memory.data===proposed.statement)).toEqual([
      expect.objectContaining({status:'active',revision:'2'})
    ]);
    expect(recovered.memories.every(memory=>memory.status==='active'&&memory.archived_at===null)).toBe(true);
    expect(recovered.embeddings).toHaveLength(2);
    expect(recovered.edges).toEqual(before.edges);
    expect(recovered.actions).toBe(before.actions!+1);
    expect(recovered.mutations).toBe(before.mutations!+1);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1 ORDER BY validation_status',[f.vault])).rows)
      .toEqual([{validation_status:'applied'},{validation_status:'invalid'}]);
    expect(await usage(f.vault)).toMatchObject({curator_requests:2,curator_input_tokens:200,curator_output_tokens:100,curator_candidates_processed:2});
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT id FROM curation_dead_letter WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
    const recoveredUsage=await usage(f.vault),embeddingCalls=state.embed.mock.calls.length;
    await makeFixtureDue(f.vault);await worker.processBatch();
    expect(call).toHaveBeenCalledTimes(2);expect(state.embed).toHaveBeenCalledTimes(embeddingCalls);
    expect(await semanticState(f.vault)).toEqual(recovered);expect(await usage(f.vault)).toEqual(recoveredUsage);
  });

  it('dead-letters consistently malformed plans at the existing limit while preserving active knowledge and accounting each attempt once',async()=>{
    const f=await fixture(),retryLimit=Number(process.env.MAX_CURATION_RETRIES??5);
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'supports',0.8,'Existing evidence')",[f.vault,...f.ids]);
    const before=await semanticState(f.vault);
    const call=model(targets=>({...empty(),keep:[{id:targets[0].id,reason:'Already useful'}],
      update:[{id:targets[1].id,memory:'Invalid replacement shape',source_refs:[targets[1].id],reason:'Malformed action'}]}));
    for(let attempt=1;attempt<=retryLimit;attempt++){
      await makeFixtureDue(f.vault);await worker.processBatch();
      expect(call).toHaveBeenCalledTimes(attempt);expect(state.embed).not.toHaveBeenCalled();
      expect(await semanticState(f.vault)).toEqual(before);
      expect((await pool.query('SELECT retry_count,claim_token FROM curation_queue WHERE id=$1',[f.queue])).rows)
        .toEqual([{retry_count:attempt,claim_token:null}]);
      expect((await pool.query("SELECT id FROM curation_review_runs WHERE vault_id=$1 AND validation_status='invalid'",[f.vault])).rowCount).toBe(attempt);
      expect(await usage(f.vault)).toMatchObject({curator_requests:attempt,curator_input_tokens:100*attempt,curator_output_tokens:50*attempt,curator_candidates_processed:0});
    }
    // The next claim retires exhausted work without another paid provider call.
    await makeFixtureDue(f.vault);await worker.processBatch();
    expect(call).toHaveBeenCalledTimes(retryLimit);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
    const dead=(await pool.query('SELECT source_queue_id,retry_count,targets FROM curation_dead_letter WHERE vault_id=$1',[f.vault])).rows;
    expect(dead).toHaveLength(1);expect(dead[0]).toMatchObject({source_queue_id:f.queue,retry_count:retryLimit});
    expect(dead[0].targets).toHaveLength(2);
    expect(dead[0].targets).toEqual(expect.arrayContaining(f.ids.map(memory_id=>({memory_id,revision:'1'}))));
    const terminalUsage=await usage(f.vault);
    await makeFixtureDue(f.vault);await worker.processBatch();
    expect(call).toHaveBeenCalledTimes(retryLimit);expect(state.embed).not.toHaveBeenCalled();
    expect(await semanticState(f.vault)).toEqual(before);expect(await usage(f.vault)).toEqual(terminalUsage);
    expect((await pool.query('SELECT id FROM curation_dead_letter WHERE vault_id=$1',[f.vault])).rowCount).toBe(1);
  });

  it('baseline enqueue completes while apply holds its old queue and waits on oppositely ordered memory writes',async()=>{
    const f=await fixture(2),baseline=await pool.connect();
    let held!:()=>void;const ready=new Promise<void>(resolve=>{held=resolve;});
    let changedQueue:string|null=null;
    model(targets=>({...empty(),update:targets.map(target=>({id:target.id,memory:proposed,
      source_refs:[target.id],reason:'Supported refinement'}))}));
    state.embed.mockImplementationOnce(async()=>{
      await baseline.query('BEGIN');
      await baseline.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE',[f.vault]);
      for(const id of [...f.ids].sort().reverse())await baseline.query("UPDATE memories SET data=data||' corrected' WHERE id=$1",[id]);
      held();return vector;
    });
    const running=worker.processBatch();
    try{
      await ready;
      const pid=(await baseline.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      let blocked=false;const deadline=Date.now()+5000;
      // A database-observed barrier: no timing guess that apply has acquired its lease.
      while(!blocked&&Date.now()<deadline){
        blocked=(await pool.query('SELECT 1 FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid))',[pid])).rowCount!>0;
      }
      expect(blocked).toBe(true);
      const probe=await pool.connect();
      try{
        await probe.query('BEGIN');
        await expect(probe.query('SELECT id FROM curation_queue WHERE id=$1 FOR UPDATE NOWAIT',[f.queue])).rejects.toMatchObject({code:'55P03'});
      }finally{await probe.query('ROLLBACK');probe.release();}
      const {enqueueCurationWork}=await import('../services/curation-work');
      await baseline.query("SET LOCAL statement_timeout='3s'");
      changedQueue=await enqueueCurationWork(baseline,{vaultId:f.vault,workKey:crypto.randomUUID(),memoryIds:[...f.ids].reverse()});
      expect(changedQueue).not.toBeNull();queues.push(changedQueue!);
      await baseline.query('COMMIT');
      await running;
      expect((await pool.query('SELECT revision::text FROM curation_queue_items WHERE queue_id=$1',[changedQueue])).rows)
        .toEqual([{revision:'2'},{revision:'2'}]);
      expect((await pool.query('SELECT data FROM memories WHERE vault_id=$1',[f.vault])).rows.every(row=>row.data.endsWith(' corrected'))).toBe(true);
      expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_candidates_processed:0});
    }finally{await baseline.query('ROLLBACK');baseline.release();await running;}
  });

  it.each(['archive','delete','context-edit','downgrade','encryption','lease'] as const)
  ('refuses the complete paid plan after concurrent %s without overwriting independent state',async change=>{
    const f=await fixture(change==='context-edit'?2:1);
    if(change==='context-edit')await pool.query('DELETE FROM curation_queue_items WHERE queue_id=$1 AND memory_id=$2',[f.queue,f.ids[1]]);
    model(async targets=>{
      if(change==='archive')await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[f.ids[0]]);
      if(change==='delete')await pool.query('DELETE FROM memories WHERE id=$1',[f.ids[0]]);
      if(change==='context-edit')await pool.query("UPDATE memories SET data='Independent context revision' WHERE id=$1",[f.ids[1]]);
      if(change==='downgrade')await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1',[f.vault,JSON.stringify({curator_enabled:false})]);
      if(change==='encryption')await pool.query('UPDATE vaults SET vault_encryption_enabled=true WHERE id=$1',[f.vault]);
      if(change==='lease')await pool.query("UPDATE curation_queue SET lease_expires_at=now()-interval '1 second' WHERE id=$1",[f.queue]);
      return{...empty(),update:[{id:targets[0].id,memory:proposed,source_refs:[targets[0].id],reason:'Proposed refinement'}]};
    });
    await worker.processBatch();
    const rows=(await pool.query('SELECT id,data,status,archived_at FROM memories WHERE vault_id=$1',[f.vault])).rows;
    expect(rows.some(r=>r.data===proposed.statement)).toBe(false);
    if(change==='delete')expect(rows).toEqual([]);
    if(change==='archive')expect(rows[0].archived_at).not.toBeNull();
    if(change==='context-edit')expect(rows.find(r=>r.id===f.ids[1]).data).toBe('Independent context revision');
    expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_input_tokens:100,curator_output_tokens:50,curator_candidates_processed:0});
  });

  it('rolls back replacement, embedding, graph and source retirement together on a late SQL failure',async()=>{
    const f=await fixture();const fn='restoration_fail_retirement',trigger='restoration_fail_retirement';
    model(targets=>({...empty(),consolidate:[{id:'N1',sources:targets.map(t=>t.id),memory:proposed,reason:'Equivalent facts'}]}));
    await pool.query(`CREATE FUNCTION ${fn}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.vault_id='${f.vault}'::uuid AND NEW.status='superseded' THEN RAISE EXCEPTION 'Synthetic late apply failure'; END IF;
      RETURN NEW; END $$`);
    await pool.query(`CREATE TRIGGER ${trigger} BEFORE UPDATE ON memories FOR EACH ROW EXECUTE FUNCTION ${fn}()`);
    try{
      await worker.processBatch();
      expect((await pool.query('SELECT data,status FROM memories WHERE vault_id=$1 ORDER BY data',[f.vault])).rows)
        .toEqual([{data:'Durable fact 0',status:'active'},{data:'Durable fact 1',status:'active'}]);
      expect((await pool.query('SELECT id FROM curation_action_log WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
      expect((await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[f.vault])).rowCount).toBe(2);
      expect((await pool.query('SELECT memory_id FROM curation_queue_items WHERE vault_id=$1',[f.vault])).rowCount).toBe(2);
      expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_candidates_processed:0});
    }finally{await pool.query(`DROP TRIGGER ${trigger} ON memories`);await pool.query(`DROP FUNCTION ${fn}()`);}
  });

  it('defers an over-budget incident graph without partially applying a complete plan',async()=>{
    const f=await fixture(1);
    const others=await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,status,scope,scope_key)
      SELECT $1,'Independent fact '||n,'Other subject '||n,gen_random_uuid()::text,'active','session','session1'
      FROM generate_series(1,501) n RETURNING id`,[f.vault]);
    await pool.query(`INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason)
      SELECT $1,$2,id,'supports',0.5,'Existing relation' FROM unnest($3::uuid[]) AS id`,[f.vault,f.ids[0],others.rows.map(r=>r.id)]);
    model(targets=>({...empty(),update:[{id:targets[0].id,memory:proposed,source_refs:[targets[0].id],reason:'Refine'}]}));
    await worker.processBatch();
    expect((await pool.query('SELECT data,revision::text FROM memories WHERE id=$1',[f.ids[0]])).rows[0]).toEqual({data:'Durable fact 0',revision:'1'});
    expect((await pool.query('SELECT id FROM memory_edges WHERE vault_id=$1',[f.vault])).rowCount).toBe(501);
    expect(await usage(f.vault)).toMatchObject({curator_requests:1,curator_candidates_processed:0});
  });
});
