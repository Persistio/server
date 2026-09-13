import crypto from 'node:crypto';
import Fastify from 'fastify';
import {Pool} from 'pg';
import {beforeAll,beforeEach,afterEach,afterAll,describe,it,expect,vi} from 'vitest';
import {registerPlatformErrorHandler} from '../http-error-handler';
const state=vi.hoisted(()=>({vector:[] as number[],embed:vi.fn()}));
vi.mock('../services/embedder',()=>({getEmbedder:()=>({embed:state.embed})}));
const url=process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!url)('actual vault-key memory authority (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:url}),app=Fastify();
  let db:typeof import('../db/client');
  const vaults:string[]=[],keys:string[]=[];
  const hash=(key:string)=>crypto.createHash('sha256').update(key).digest('hex');
  beforeAll(async()=>{
    db=await import('../db/client');await db.runMigrations();
    const config=(await import('../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    state.vector=[1,...Array(config.STORAGE_EMBEDDING_DIMENSIONS-1).fill(0)];
    registerPlatformErrorHandler(app);
    await(await import('./memories')).registerMemoryRoutes(app);await(await import('./recall')).registerRecallRoutes(app);
  });
  beforeEach(async()=>{
    state.embed.mockReset().mockResolvedValue(state.vector);
    for(let i=0;i<2;i++){
      const id=crypto.randomUUID(),key='synthetic-vault-key-'+crypto.randomUUID();vaults.push(id);keys.push(key);
      await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'auth-boundary',$2,'unlimited')",[id,hash(key)]);
    }
  });
  afterEach(async()=>{await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;keys.length=0;});
  afterAll(async()=>{await app.close();await pool.end();await db?.closePool();});
  const headers=(key?:string)=>key?{authorization:'Bearer '+key}:{};
  const create=(key?:string,extra:Record<string,unknown>={})=>app.inject({method:'POST',url:'/v1/memories',headers:headers(key),
    payload:{data:'Useful durable owner-authored fact',subject:'Topic',scope:'session',scope_key:'s1',...extra}});
  const recall=(key?:string)=>app.inject({method:'POST',url:'/v1/recall',headers:headers(key),payload:{query:'Useful fact',context:{session_id:'s1'}}});
  it('allows the same full vault key to create active knowledge and recall it without approval',async()=>{
    const added=await create(keys[0]);expect(added.statusCode,added.body).toBe(201);
    const row=added.json();expect(row.status).toBe('active');expect(row).not.toHaveProperty('authority_state');
    const found=await recall(keys[0]);expect(found.statusCode,found.body).toBe(200);
    expect(found.json().memories.map((m:any)=>m.id)).toEqual([row.id]);
    const evidence=(await pool.query('SELECT evidence FROM memories WHERE id=$1',[row.id])).rows[0].evidence;
    expect(evidence.actor.id).toBe('vault:'+vaults[0]);
  });
  it('rejects missing, invalid and admin credentials for both direct memory capabilities',async()=>{
    for(const key of [undefined,'invalid-synthetic-key',process.env.ADMIN_API_KEY,'eyJhbGciOiJub25lIn0.eyJzY29wZSI6InZhdWx0OmFjY2VzcyJ9.']){
      expect((await create(key)).statusCode).toBe(401);expect((await recall(key)).statusCode).toBe(401);
    }
    expect(state.embed).not.toHaveBeenCalled();
  });
  it('does not let a different valid vault key read, patch, archive or override ownership',async()=>{
    const m=(await create(keys[0])).json();state.embed.mockClear();
    expect((await app.inject({method:'GET',url:'/v1/memories/'+m.id,headers:headers(keys[1])})).statusCode).toBe(404);
    expect((await app.inject({method:'PATCH',url:'/v1/memories/'+m.id,headers:headers(keys[1]),payload:{data:'Foreign change'}})).statusCode).toBe(404);
    expect((await app.inject({method:'DELETE',url:'/v1/memories/'+m.id,headers:headers(keys[1])})).statusCode).toBe(404);
    for(const extra of [{vault_id:vaults[0]},{actor:{id:'admin'}},{authority_state:'approved'}])expect((await create(keys[1],extra)).statusCode).toBe(400);
    expect(state.embed).not.toHaveBeenCalled();
    expect((await recall(keys[1])).json().memories).toEqual([]);
    expect((await pool.query('SELECT data,status,archived_at FROM memories WHERE id=$1',[m.id])).rows[0])
      .toEqual({data:'Useful durable owner-authored fact',status:'active',archived_at:null});
  });
  it('rejects the old key after rotation and keys of inactive vaults',async()=>{
    const replacement='replacement-'+crypto.randomUUID();
    await pool.query('UPDATE vaults SET api_key_hash=$2 WHERE id=$1',[vaults[0],hash(replacement)]);
    expect((await create(keys[0])).statusCode).toBe(401);expect((await recall(keys[0])).statusCode).toBe(401);
    expect((await create(replacement)).statusCode).toBe(201);
    await pool.query("UPDATE vaults SET status='disabled' WHERE id=$1",[vaults[0]]);
    expect((await create(replacement)).statusCode).toBe(401);expect((await recall(replacement)).statusCode).toBe(401);
  });
});
