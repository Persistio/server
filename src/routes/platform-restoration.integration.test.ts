import crypto from 'node:crypto';
import Fastify,{type FastifyRequest} from 'fastify';
import {Pool} from 'pg';
import {beforeAll,afterAll,afterEach,beforeEach,describe,it,expect,vi} from 'vitest';
import {registerPlatformErrorHandler} from '../http-error-handler';
const state=vi.hoisted(()=>({vault:null as any,vector:[] as number[],embedCalls:0,beforeEmbed:null as null|(()=>Promise<void>)}));
vi.mock('../middleware/auth',()=>({
  requireVaultReadAuth:async(request:FastifyRequest)=>{request.vault=state.vault;request.auth={method:'api_key'} as any;},
  requireVaultWriteAuth:async(request:FastifyRequest)=>{request.vault=state.vault;request.auth={method:'api_key'} as any;},
  requireAdminScope:()=>async()=>{},getAuthAccountId:()=>null
}));
vi.mock('../services/embedder',()=>({getEmbedder:()=>({embed:async()=>{state.embedCalls++;await state.beforeEmbed?.();return state.vector;}})}));
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('restored standalone HTTP API (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl}),app=Fastify();
  let db:typeof import('../db/client');
  beforeAll(async()=>{
    registerPlatformErrorHandler(app);
    process.env.DATABASE_URL=databaseUrl;db=await import('../db/client');await db.runMigrations();
    const config=(await import('../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    const type=(await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    state.vector=[1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)];
    await(await import('./memories')).registerMemoryRoutes(app);await(await import('./recall')).registerRecallRoutes(app);
  });
  beforeEach(async()=>{
    state.embedCalls=0;
    const id=crypto.randomUUID();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'standalone-restoration',$2,'unlimited')",[id,crypto.randomUUID()]);
    state.vault=(await pool.query('SELECT * FROM vaults WHERE id=$1',[id])).rows[0];
  });
  afterEach(async()=>{vi.restoreAllMocks();state.beforeEmbed=null;await pool.query('DELETE FROM vaults WHERE id=$1',[state.vault.id]);});
  afterAll(async()=>{await app.close();await pool.end();await db?.closePool();});
  async function add(data='Useful durable fact',extra:Record<string,unknown>={}){
    const result=await app.inject({method:'POST',url:'/v1/memories',payload:{data,subject:'Topic',scope:'session',scope_key:'s1',...extra}});
    expect(result.statusCode,result.body).toBe(201);return result.json();
  }
  const recall=(body:Record<string,unknown>={},format='')=>app.inject({method:'POST',url:'/v1/recall'+format,
    payload:{query:'Relevant topic',context:{session_id:'s1'},...body}});
  it('manual baseline is active and recallable before Curator runs',async()=>{
    const m=await add();expect(m.status).toBe('active');expect(m).not.toHaveProperty('authority_state');
    const res=await recall();expect(res.statusCode,res.body).toBe(200);expect(res.json().memories.map((m:any)=>m.id)).toContain(m.id);
    expect((await pool.query('SELECT memory_id FROM curation_queue_items WHERE vault_id=$1',[state.vault.id])).rows).toEqual([{memory_id:m.id}]);
    expect((await pool.query('SELECT memory_id FROM memory_embeddings WHERE memory_id=$1',[m.id])).rowCount).toBe(1);
  });
  it('baseline plan does not enqueue premium work and produces the same usable memory',async()=>{
    await pool.query("UPDATE vaults SET rate_limit_override='{"+'"curator_enabled":false'+"}' WHERE id=$1",[state.vault.id]);
    const m=await add();expect((await recall()).json().memories.map((r:any)=>r.id)).toContain(m.id);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[state.vault.id])).rowCount).toBe(0);
  });
  it.each([0,100,1200,65536])('returns a complete server-built bundle within %i UTF-8 bytes without ACK',async budget=>{
    await add('Preference with multibyte text 日本語 and </persistio_context>');
    const res=await recall({max_bundle_bytes:budget},'?format=bundle_v3');expect(res.statusCode,res.body).toBe(200);
    const value=res.json();expect(Object.keys(value).sort()).toEqual(['bundle','schema_version']);
    expect(Buffer.byteLength(value.bundle,'utf8')).toBeLessThanOrEqual(budget);
    if(value.bundle)expect(value.bundle.endsWith('</persistio_context>')).toBe(true);
  });
  it('rejects legacy pending/formats and has no delivery endpoint',async()=>{
    expect((await recall({include_pending:true})).statusCode).toBe(400);
    for(const format of ['bundle','bundle_v2'])expect((await recall({},'?format='+format)).statusCode).toBe(400);
    expect((await app.inject({method:'POST',url:'/v1/recall/deliveries/'+crypto.randomUUID()+'/rendered',payload:{}})).statusCode).toBe(404);
  });
  it('keeps ordinary historical facts recallable and respects exact binding',async()=>{
    const m=await add('The project completed in 2020');
    await pool.query("UPDATE memories SET valid_from='2020-01-01',valid_until='2020-12-31' WHERE id=$1",[m.id]);
    expect((await recall()).json().memories.map((r:any)=>r.id)).toContain(m.id);
    expect((await recall({context:{session_id:'s2'}})).json().memories).toEqual([]);
    expect((await recall({},'?format=bundle_v3')).json().bundle).toContain('historical');
  });
  it('recalls vault-wide preferences by relevance across sessions, without reserved rule slots',async()=>{
    const preference=await add('Use UK English',{type:'user_preference',scope:'global',scope_key:null});
    const rule=await add('An unrelated rule',{type:'user_rule',scope:'global',scope_key:null});
    await pool.query('UPDATE memories SET salience=1 WHERE id=$1',[rule.id]);
    const orthogonal=[0,1,...state.vector.slice(2)];
    await pool.query('UPDATE memory_embeddings SET embedding=$2::vector WHERE memory_id=$1',[rule.id,JSON.stringify(orthogonal)]);
    const res=await recall({top_k:1,min_similarity:0.5,context:{session_id:'s2'}});
    expect(res.statusCode,res.body).toBe(200);
    expect(res.json().memories.map((m:any)=>m.id)).toEqual([preference.id]);
    expect(res.json().memories[0].source).toBe('semantic');
    expect((await recall({include_global_rules:false})).statusCode).toBe(400);
    expect((await recall({include_global_rules:true})).statusCode).toBe(400);
    await pool.query('UPDATE memory_embeddings SET embedding=$2::vector WHERE memory_id=$1',[rule.id,JSON.stringify(state.vector)]);
    await pool.query("UPDATE memories SET valid_until='2020-01-01' WHERE id=$1",[rule.id]);
    const historical=await recall({min_similarity:0.5,context:{session_id:'s2'}});
    expect(historical.json().memories.some((m:any)=>m.id===rule.id)).toBe(true);
    const bundle=await recall({min_similarity:0.5,max_bundle_bytes:4096,context:{session_id:'s2'}},'?format=bundle_v3');
    expect(bundle.json().bundle).toContain('historical');
  });
  it('expands only same-binding graph neighbours of relevant seeds under one budget',async()=>{
    const seed=await add('Relevant vault knowledge',{scope:'global',scope_key:null});
    const neighbour=await add('Related preference',{type:'user_preference',scope:'global',scope_key:null});
    const unrelated=await add('Unrelated standing instruction',{type:'user_rule',scope:'global',scope_key:null});
    const otherScope=await add('Different project',{scope:'project',scope_key:'p2'});
    const orthogonal=[0,1,...state.vector.slice(2)];
    await pool.query('UPDATE memory_embeddings SET embedding=$2::vector WHERE memory_id=ANY($1::uuid[])',
      [[neighbour.id,unrelated.id,otherScope.id],JSON.stringify(orthogonal)]);
    await pool.query(`INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence)
      VALUES($1,$2,$3,'supports',0.9)`,[state.vault.id,seed.id,neighbour.id]);
    const res=await recall({top_k:2,min_similarity:0.5,context:{session_id:'s2'}});
    expect(res.statusCode,res.body).toBe(200);
    expect(res.json().memories.map((m:any)=>m.id)).toEqual([seed.id]);
    expect(res.json().related_memories.map((m:any)=>m.id)).toEqual([neighbour.id]);
    expect((await recall({top_k:1,min_similarity:0.5})).json().related_memories).toEqual([]);
    await pool.query("UPDATE vaults SET rate_limit_override='{\"curator_enabled\":false}' WHERE id=$1",[state.vault.id]);
    expect((await recall({top_k:2,min_similarity:0.5})).json().related_memories).toEqual([]);
  });
  it('returns explicit uncertainty for a revision-matched unresolved pair, even if only one fits',async()=>{
    const a=await add('First supported alternative'),b=await add('Second supported alternative');
    await pool.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b)
      SELECT $1,a.id,b.id,'keep_both',0.9,a.revision,b.revision FROM memories a,memories b WHERE a.id=$2 AND b.id=$3`,[state.vault.id,a.id,b.id]);
    const res=await recall({top_k:1},'?format=bundle_v3');expect(res.json().bundle).toContain('Unresolved conflicting evidence');
    await pool.query("UPDATE memories SET data='Resolved revised fact' WHERE id=$1",[b.id]);
    expect((await recall({top_k:1},'?format=bundle_v3')).json().bundle).not.toContain('Unresolved conflicting evidence');
  });
  it('edits atomically with embedding and new revision membership, and rejects approval fields',async()=>{
    const m=await add();const patch=await app.inject({method:'PATCH',url:'/v1/memories/'+m.id,payload:{data:'Useful correction'}});
    expect(patch.statusCode,patch.body).toBe(200);
    expect((await pool.query('SELECT revision::text FROM curation_queue_items WHERE memory_id=$1 ORDER BY revision',[m.id])).rows).toEqual([{revision:'1'},{revision:'2'}]);
    expect((await app.inject({method:'PATCH',url:'/v1/memories/'+m.id,payload:{authority_state:'approved'}})).statusCode).toBe(400);
    expect((await app.inject({method:'PATCH',url:'/v1/memories/'+m.id,payload:{scope:'global',scope_change_reason:'Explicit cross-conversation authoring'}})).statusCode).toBe(200);
    expect((await pool.query('SELECT new_scope FROM memory_scope_change_log WHERE memory_id=$1',[m.id])).rows).toEqual([{new_scope:'global'}]);
  });
  it('rejects invalid parents without charging or leaving a partial memory',async()=>{
    const parent=await add('Parent',{scope:'project',scope_key:'p1'});
    const res=await app.inject({method:'POST',url:'/v1/memories',payload:{data:'Child',subject:'Topic',scope:'session',scope_key:'s1',parent_id:parent.id}});
    expect(res.statusCode).toBe(400);expect((await pool.query('SELECT id FROM memories WHERE vault_id=$1',[state.vault.id])).rowCount).toBe(1);
  });
  const patch=(id:string,payload:Record<string,unknown>)=>app.inject({method:'PATCH',url:'/v1/memories/'+id,payload});
  it.each(['session','task','project'])('never inherits a %s binding into another namespace',async scope=>{
    for(const target of ['session','task','project'].filter(value=>value!==scope)){
      const m=await add('Scoped fact',{scope,scope_key:'same-text'});
      const count=state.embedCalls;
      const res=await patch(m.id,{scope:target,data:'Changed fact',scope_change_reason:'Explicit intent'});
      expect(res.statusCode,res.body).toBe(400);
      expect(state.embedCalls).toBe(count);
      expect((await pool.query('SELECT scope,scope_key,revision::text FROM memories WHERE id=$1',[m.id])).rows[0])
        .toEqual({scope,scope_key:'same-text',revision:'1'});
      expect((await patch(m.id,{scope:target,scope_key:'same-text',scope_change_reason:'Explicit binding'})).statusCode).toBe(200);
    }
  });
  it.each([{memories_max:0},{memory_adds_per_month:0}])('rejects exhausted %j before embedding or accounting',async limits=>{
    await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1',[state.vault.id,JSON.stringify(limits)]);
    const res=await app.inject({method:'POST',url:'/v1/memories',payload:{data:'Useful fact',subject:'Topic',scope:'session',scope_key:'s1'}});
    expect(res.statusCode,res.body).toBe(429);expect(state.embedCalls).toBe(0);
    expect((await pool.query('SELECT id FROM memories WHERE vault_id=$1',[state.vault.id])).rowCount).toBe(0);
    expect((await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1',[state.vault.id])).rows).toEqual([]);
  });
  it('rejects missing IDs and secret-bearing PATCHes before embedding',async()=>{
    expect((await patch(crypto.randomUUID(),{data:'Missing target'})).statusCode).toBe(404);
    expect(state.embedCalls).toBe(0);
    const m=await add();const calls=state.embedCalls;
    expect((await patch(m.id,{data:'API key: sk-'+ 'a'.repeat(48)})).statusCode).toBe(400);
    expect(state.embedCalls).toBe(calls);
    expect((await pool.query('SELECT revision::text FROM memories WHERE id=$1',[m.id])).rows[0].revision).toBe('1');
  });
  it('allows edits at full capacity but rejects a content-changing restoration before embedding',async()=>{
    const m=await add(),archived=await add('Archived fact');await patch(archived.id,{archived:true});
    await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1',[state.vault.id,JSON.stringify({memories_max:1,memory_adds_per_month:2})]);
    expect((await patch(m.id,{data:'Still useful correction'})).statusCode).toBe(200);
    const calls=state.embedCalls;
    expect((await patch(archived.id,{archived:false,data:'Restore correction'})).statusCode).toBe(429);
    expect(state.embedCalls).toBe(calls);
    expect((await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1',[state.vault.id])).rows[0].memory_adds).toBe(2);
  });
  it('reserves only one last slot across concurrent creates without holding a lock during embedding',async()=>{
    await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1',[state.vault.id,JSON.stringify({memories_max:1,memory_adds_per_month:1})]);
    let release!:()=>void,calls=0;const barrier=new Promise<void>(resolve=>{release=resolve;});
    state.beforeEmbed=async()=>{if(++calls===2)release();await barrier;};
    const create=(data:string)=>app.inject({method:'POST',url:'/v1/memories',payload:{data,subject:'Topic',scope:'session',scope_key:'s1'}});
    try{
      const results=await Promise.all([create('First fact'),create('Second fact')]);
      expect(results.map(r=>r.statusCode).sort()).toEqual([201,429]);
      expect((await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1',[state.vault.id])).rows[0].memory_adds).toBe(1);
      expect((await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[state.vault.id])).rowCount).toBe(1);
      expect((await pool.query('SELECT memory_id FROM curation_queue_items WHERE vault_id=$1',[state.vault.id])).rowCount).toBe(1);
    }finally{release();state.beforeEmbed=null;}
  });
  it('revalidates the namespace against locked state after a concurrent change',async()=>{
    const m=await add();
    state.beforeEmbed=async()=>{state.beforeEmbed=null;await pool.query("UPDATE memories SET scope='task',scope_key='s1' WHERE id=$1",[m.id]);};
    const res=await patch(m.id,{scope:'session',data:'Must not reuse task key',scope_change_reason:'Originally unchanged'});
    expect(res.statusCode,res.body).toBe(400);
    expect((await pool.query('SELECT scope,data,revision::text FROM memories WHERE id=$1',[m.id])).rows[0])
      .toEqual({scope:'task',data:'Useful durable fact',revision:'2'});
    expect((await pool.query('SELECT id FROM memory_scope_change_log WHERE memory_id=$1',[m.id])).rowCount).toBe(0);
  });
  it('rejects known parent/child binding conflicts before embedding and keeps links intact',async()=>{
    const parent=await add('Parent'),child=await add('Child',{parent_id:parent.id});
    const calls=state.embedCalls;
    for(const m of [parent,child])expect((await patch(m.id,{scope:'global',data:'Cannot disconnect',scope_change_reason:'Explicit change'})).statusCode).toBe(409);
    expect(state.embedCalls).toBe(calls);
    expect((await pool.query('SELECT parent_id FROM memories WHERE id=$1',[child.id])).rows[0].parent_id).toBe(parent.id);
  });
  it.each(['session','task','project'])('explicitly changes %s binding to global for omitted or null keys with transactional audit',async scope=>{
    for(const key of [{},{scope_key:null}]){
      const m=await add('Explicit scope change',{scope,scope_key:'original'});
      expect((await patch(m.id,{scope:'global',...key})).statusCode).toBe(400);
      const res=await patch(m.id,{scope:'global',...key,scope_change_reason:'Explicit user cross-conversation intent'});
      expect(res.statusCode,res.body).toBe(200);expect(res.json()).toMatchObject({scope:'global',scope_key:null});
      expect((await pool.query('SELECT old_scope,new_scope,old_scope_key,new_scope_key FROM memory_scope_change_log WHERE memory_id=$1',[m.id])).rows)
        .toEqual([{old_scope:scope,new_scope:'global',old_scope_key:'original',new_scope_key:null}]);
      for(const field of ['encrypted_dek','embedding','previous_scope','scope_change_id'])expect(res.json()).not.toHaveProperty(field);
    }
  });
  it('rejects key-only rebinding of a global memory without altering the original',async()=>{
    const m=await add('Global fact',{scope:'global',scope_key:null});
    expect((await patch(m.id,{scope_key:'other',scope_change_reason:'Reason'})).statusCode).toBe(400);
    expect((await pool.query('SELECT scope,scope_key,revision::text FROM memories WHERE id=$1',[m.id])).rows[0]).toEqual({scope:'global',scope_key:null,revision:'1'});
  });
  it('preserves omitted evidence and explicitly clears its summary without destroying author metadata',async()=>{
    const m=await add('Evidence test',{evidence:'Original supporting evidence'});
    expect((await patch(m.id,{confidence:0.8})).statusCode).toBe(200);
    const preserved=(await pool.query('SELECT evidence FROM memories WHERE id=$1',[m.id])).rows[0].evidence;
    expect(preserved.summary).toBe('Original supporting evidence');
    expect((await patch(m.id,{evidence:null})).statusCode).toBe(200);
    expect((await pool.query('SELECT evidence FROM memories WHERE id=$1',[m.id])).rows[0].evidence).toMatchObject({summary:null,authored_via:'memory_api'});
  });
  it('serializes repeated archive requests and only restores within retained capacity',async()=>{
    const m=await add();
    const responses=await Promise.all([app.inject({method:'DELETE',url:'/v1/memories/'+m.id}),app.inject({method:'DELETE',url:'/v1/memories/'+m.id})]);
    expect(responses.map(r=>r.statusCode)).toEqual([200,200]);
    expect((await pool.query('SELECT revision::text FROM memories WHERE id=$1',[m.id])).rows[0].revision).toBe('2');
    await add('Occupies remaining capacity');
    await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1',[state.vault.id,JSON.stringify({memories_max:1})]);
    const denied=await patch(m.id,{archived:false});expect(denied.statusCode,denied.body).toBe(429);
    expect((await pool.query('SELECT archived_at FROM memories WHERE id=$1',[m.id])).rows[0].archived_at).not.toBeNull();
    await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1',[state.vault.id,JSON.stringify({memories_max:2})]);
    expect((await patch(m.id,{archived:false})).statusCode).toBe(200);
    expect((await pool.query('SELECT status,archived_at FROM memories WHERE id=$1',[m.id])).rows[0]).toEqual({status:'active',archived_at:null});
  });
  it('derives scope auditing from locked current state after provider preparation, not a stale read',async()=>{
    const m=await add();
    state.beforeEmbed=async()=>{await pool.query("UPDATE memories SET scope_key='independent-binding' WHERE id=$1",[m.id]);state.beforeEmbed=null;};
    const res=await patch(m.id,{data:'Clarified fact',scope:'global',scope_change_reason:'Explicit broadened intent'});
    expect(res.statusCode,res.body).toBe(200);
    expect((await pool.query('SELECT old_scope_key,new_scope_key FROM memory_scope_change_log WHERE memory_id=$1',[m.id])).rows)
      .toEqual([{old_scope_key:'independent-binding',new_scope_key:null}]);
  });
  it('rejects changed encryption identity before applying provider-prepared writes',async()=>{
    const m=await add();
    state.beforeEmbed=async()=>{await pool.query('UPDATE vaults SET vault_encryption_enabled=true WHERE id=$1',[state.vault.id]);state.beforeEmbed=null;};
    const res=await patch(m.id,{data:'Must not be committed'});expect(res.statusCode).toBeGreaterThanOrEqual(400);
    expect((await pool.query('SELECT data,revision::text FROM memories WHERE id=$1',[m.id])).rows[0]).toEqual({data:'Useful durable fact',revision:'1'});
  });
  it('rolls back memory, archive, audit and queue changes if embedding sync fails',async()=>{
    const m=await add();
    const fn='restoration_api_embedding_failure';
    await pool.query(`CREATE FUNCTION ${fn}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.memory_id='${m.id}'::uuid THEN RAISE EXCEPTION 'Synthetic embedding write failure'; END IF; RETURN NEW; END $$`);
    await pool.query(`CREATE TRIGGER ${fn} BEFORE INSERT OR UPDATE ON memory_embeddings FOR EACH ROW EXECUTE FUNCTION ${fn}()`);
    try{
      const res=await patch(m.id,{data:'Must rollback',archived:true});expect(res.statusCode).toBe(500);
      expect((await pool.query('SELECT data,archived_at,revision::text FROM memories WHERE id=$1',[m.id])).rows[0])
        .toEqual({data:'Useful durable fact',archived_at:null,revision:'1'});
      expect((await pool.query('SELECT id FROM memory_mutation_events WHERE memory_id=$1',[m.id])).rowCount).toBe(1);
      expect((await pool.query('SELECT revision::text FROM curation_queue_items WHERE memory_id=$1',[m.id])).rows).toEqual([{revision:'1'}]);
    }finally{await pool.query(`DROP TRIGGER ${fn} ON memory_embeddings`);await pool.query(`DROP FUNCTION ${fn}()`);}
  });
  it('has no authority mutation endpoints and rejects pending fields across read surfaces',async()=>{
    const m=await add();
    for(const action of ['approve','revoke'])expect((await app.inject({method:'POST',url:'/v1/memories/'+m.id+'/'+action,payload:{}})).statusCode).toBe(404);
    for(const url of ['/v1/memories','/v1/memories/'+m.id]){
      // The production error handler maps schema failures to 400. This isolated
      // route instance must still reject, never silently expose another lane.
      expect((await app.inject({method:'GET',url:url+'?include_pending=true'})).statusCode).toBeGreaterThanOrEqual(400);
    }
  });
});
