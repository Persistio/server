import crypto from 'node:crypto';
import Fastify from 'fastify';
import {Pool} from 'pg';
import {beforeAll,afterAll,afterEach,beforeEach,describe,it,expect,vi} from 'vitest';
import {registerPlatformErrorHandler} from '../http-error-handler';
const state=vi.hoisted(()=>({vault:null as any,vector:[] as number[],blobs:new Map<string,string>(),read:vi.fn()}));
vi.mock('../middleware/auth',()=>({requireVaultReadAuth:async(request:any)=>{request.vault=state.vault;}}));
vi.mock('../services/embedder',()=>({getEmbedder:()=>({embed:async()=>state.vector})}));
vi.mock('../services/raw-chunk-storage',()=>({getRawChunkStorage:()=>({store:'local',get:state.read})}));
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('explicit source recall isolation (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl}),app=Fastify(),vaults:string[]=[];
  let db:typeof import('../db/client');
  beforeAll(async()=>{
    registerPlatformErrorHandler(app);
    process.env.DATABASE_URL=databaseUrl;db=await import('../db/client');await db.runMigrations();
    const config=(await import('../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    state.vector=[1,...Array(config.STORAGE_EMBEDDING_DIMENSIONS-1).fill(0)];
    await(await import('./recall')).registerRecallRoutes(app);
  });
  beforeEach(async()=>{
    for(let i=0;i<2;i++){
      const id=crypto.randomUUID();vaults.push(id);
      await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES ($1,'source-isolation',$2,'unlimited')",[id,crypto.randomUUID()]);
    }
    state.vault=(await pool.query('SELECT * FROM vaults WHERE id=$1',[vaults[0]])).rows[0];
    state.blobs.clear();state.read.mockReset().mockImplementation(async(key:string)=>state.blobs.get(key)!);
  });
  afterEach(async()=>{await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;});
  afterAll(async()=>{await app.close();await pool.end();await db?.closePool();});
  const recall=(body:any={},format='')=>app.inject({method:'POST',url:'/v1/recall'+format,
    payload:{query:'source fact',context:{session_id:'s1',project_id:'p1',task_id:'t1'},include_raw:true,include_evidence:true,...body}});
  async function source(options:{vault?:string;session?:string;project?:string|null;task?:string|null;content?:string;store?:string;bytes?:number;vector?:number[]|null}={}){
    const id=crypto.randomUUID(),content=options.content??'Supported source fact';state.blobs.set(id,content);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_key,blob_store,storage_bytes,capture_context,embedding) VALUES ($1::uuid,$2,$3,'user',$1::text,$4,$5,$6::jsonb,$7::vector)",
      [id,options.vault??vaults[0],options.session??'s1',options.store??'local',options.bytes??Buffer.byteLength(content),
        JSON.stringify({project_id:options.project===undefined?'p1':options.project,task_id:options.task===undefined?'t1':options.task}),options.vector===null?null:JSON.stringify(options.vector??state.vector)]);
    return id;
  }
  async function memory(sources:string[]){
    const id=crypto.randomUUID();
    await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,status,type,scope,scope_key,source_chunks,embedding) VALUES ($1,$2,'Durable fact','source',$3,'active','system_fact','session','s1',$4::uuid[],$5::vector)",
      [id,vaults[0],crypto.randomUUID(),sources,JSON.stringify(state.vector)]);
    await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES ($1,$2::vector)',[id,JSON.stringify(state.vector)]);
  }
  it('bounds both source lanes to the exact vault/session/project/task, including forged foreign lineage',async()=>{
    const evidence=await source(),raw=await source();
    const denied=[await source({vault:vaults[1]}),await source({session:'s2'}),await source({project:'p2'}),await source({task:'t2'}),await source({project:null}),await source({store:'gcs'})];
    // The storage boundary rejects forged lineage before recall runs.
    await expect(memory([evidence,...denied])).rejects.toThrow(/must all belong to vault/);
    await memory([evidence,...denied.slice(1)]);
    const response=await recall();expect(response.statusCode,response.body).toBe(200);
    expect(response.json().evidence_chunks.map((r:any)=>r.id)).toEqual([evidence]);
    expect(response.json().raw_chunks.map((r:any)=>r.id)).toEqual([raw]);
    expect(state.read.mock.calls.map(([key])=>key).sort()).toEqual([evidence,raw].sort());
  });
  it('never reads sources unless explicitly requested with a session, and never mixes them into a bundle',async()=>{
    await memory([await source()]);
    expect((await recall({include_raw:false,include_evidence:false})).statusCode).toBe(200);
    expect(state.read).not.toHaveBeenCalled();
    expect((await recall({context:{project_id:'p1'}})).statusCode).toBe(400);
    expect((await recall({},'?format=bundle_v3')).statusCode).toBe(400);
    expect(state.read).not.toHaveBeenCalled();
  });
  it('prioritizes supporting evidence without a vector over newer raw matches under one cap',async()=>{
    const evidence=await source({vector:null});await memory([evidence]);
    for(let i=0;i<10;i++)await source();
    const res=await recall({top_k:1});expect(res.statusCode,res.body).toBe(200);
    expect(res.json().evidence_chunks.map((s:any)=>s.id)).toEqual([evidence]);
    expect(res.json().raw_chunks).toEqual([]);expect(state.read).toHaveBeenCalledOnce();
  });
  it('ranks raw-only matches by similarity, excluding unreadable stores before its limit',async()=>{
    const strongest=await source();
    await source({vector:[0.8,0.6,...state.vector.slice(2)]});
    await source({store:'gcs'});
    const res=await recall({include_evidence:false,top_k:1});expect(res.statusCode,res.body).toBe(200);
    expect(res.json().raw_chunks.map((s:any)=>s.id)).toEqual([strongest]);
    expect(state.read.mock.calls).toEqual([[strongest]]);
  });
  it.each([[false,false],[false,true],[true,false],[true,true]])('keeps raw=%s/evidence=%s distinct with deterministic ties and no duplicates',async(include_raw,include_evidence)=>{
    const a=await source(),b=await source();await memory([a,a]);
    const res=await recall({include_raw,include_evidence,top_k:2});expect(res.statusCode,res.body).toBe(200);
    expect(res.json().evidence_chunks.map((r:any)=>r.id)).toEqual(include_evidence?[a]:[]);
    expect(res.json().raw_chunks.map((r:any)=>r.id)).toEqual(!include_raw?[]:include_evidence?[b]:[a,b].sort());
    expect(new Set(state.read.mock.calls.map(([key])=>key)).size).toBe(state.read.mock.calls.length);
  });
  it('shares the exact 64 KiB source allowance across evidence and raw',async()=>{
    const evidence=await source({content:'é'.repeat(16384)});await memory([evidence]);
    await source({content:'a'.repeat(32768)});await source({content:'b'.repeat(32768)});
    const res=await recall({top_k:3});expect(res.statusCode,res.body).toBe(200);
    const rows=[...res.json().evidence_chunks,...res.json().raw_chunks];
    expect(rows).toHaveLength(2);expect(rows[0].id).toBe(evidence);
    expect(rows.reduce((bytes:number,row:any)=>bytes+Buffer.byteLength(row.content),0)).toBe(65536);
    expect(state.read).toHaveBeenCalledTimes(2);
  });
  it('enforces a shared decoded byte budget, not just recorded storage length',async()=>{
    await source({content:'é'.repeat(40000),bytes:10});
    await source({content:'a'.repeat(40000)});
    await source({content:'b'.repeat(40000)});
    const response=await recall();expect(response.statusCode,response.body).toBe(200);
    const sources=[...response.json().raw_chunks,...response.json().evidence_chunks];
    expect(sources).toHaveLength(1);
    expect(sources.reduce((n:number,r:any)=>n+Buffer.byteLength(r.content),0)).toBe(40000);
  });
});
