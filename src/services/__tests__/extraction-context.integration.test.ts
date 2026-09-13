import crypto from 'node:crypto';
import {Pool} from 'pg';
import {afterAll,afterEach,beforeAll,beforeEach,describe,expect,it} from 'vitest';
import {freezeExtractionContext} from '../extraction-context';

const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('bounded eligible frozen extraction context',()=>{
  const pool=new Pool({connectionString:url});
  let db:typeof import('../../db/client'),vault:string;
  beforeAll(async()=>{db=await import('../../db/client');await db.runMigrations();});
  beforeEach(async()=>{vault=crypto.randomUUID();await pool.query("INSERT INTO vaults(id,name,api_key_hash) VALUES($1,'context-regression',$2)",[vault,crypto.randomUUID()]);});
  afterEach(async()=>{
    await pool.query('DELETE FROM extraction_queue WHERE vault_id=$1',[vault]);
    await pool.query('DELETE FROM vaults WHERE id=$1',[vault]);
  });
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function source(opts:{role?:string;key?:string|null;bytes?:number;store?:string|null;session?:string;project?:string|null;task?:string|null}={}){
    const id=crypto.randomUUID();
    await pool.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_key,blob_store,storage_bytes,capture_context)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,[id,vault,opts.session??'s1',opts.role??'user',opts.key===undefined?id:opts.key,
      opts.store===undefined?'local':opts.store,opts.bytes??1,JSON.stringify({project_id:opts.project??null,task_id:opts.task??null})]);
    return id;
  }
  async function job(){
    const current=await source();
    const queue=(await pool.query('INSERT INTO extraction_queue(vault_id,chunk_id) VALUES($1,$2) RETURNING id',[vault,current])).rows[0].id;
    const input={queueId:queue,vaultId:vault,chunkIds:[current],context:{session_id:'s1'},blobStore:'local'};
    const freeze=()=>db.withTransaction(client=>freezeExtractionContext(client,input));
    return{current,queue,freeze};
  }
  it.each(['tool','keyless','oversized','wrong-store'] as const)('selects eligible records before the cap, past %s rows',async kind=>{
    const good=await source();
    for(let i=0;i<10;i++)await source(kind==='tool'?{role:'tool'}:kind==='keyless'?{key:null}:kind==='oversized'?{bytes:32769}:{store:'gcs'});
    const f=await job();expect((await f.freeze()).map(r=>r.id)).toEqual([good]);
  });
  it('freezes at most eight eligible rows and never absorbs newer arrivals on retry',async()=>{
    const ids:string[]=[];for(let i=0;i<10;i++)ids.push(await source());
    const f=await job();expect((await f.freeze()).map(r=>r.id)).toEqual(ids.slice(-8));
    await source();await pool.query('DELETE FROM raw_chunks WHERE id=$1',[ids[9]]);
    expect((await f.freeze()).map(r=>r.id)).toEqual(ids.slice(-8,-1));
    expect((await pool.query('SELECT context_chunk_ids FROM extraction_queue WHERE id=$1',[f.queue])).rows[0].context_chunk_ids).toEqual(ids.slice(-8));
  });
  it('honors the shared recorded-byte limit and exact optional bindings',async()=>{
    await source({session:'s2'});await source({project:'p2'});await source({task:'t2'});
    const older=await source({bytes:16384});const newer=await source({bytes:16384});
    const f=await job();expect((await f.freeze()).map(r=>r.id)).toEqual([older,newer]);
  });
  it('keeps frozen-empty distinct from unfrozen and preserves null-store extraction fallback',async()=>{
    const f=await job();expect(await f.freeze()).toEqual([]);await source();expect(await f.freeze()).toEqual([]);
    const nullable=await source({store:null});const next=await job();expect((await next.freeze()).map(r=>r.id)).toContain(nullable);
  });
  it.each([null,'p1'])('uses indexed eligibility for a long ineligible history with project=%s',async project=>{
    const good=await source({project});
    await pool.query(`INSERT INTO raw_chunks(vault_id,session_id,role,blob_key,blob_store,storage_bytes,capture_context)
      SELECT $1,'s1',CASE WHEN n%2=0 THEN 'tool' ELSE 'user' END,'synthetic-'||n,'local',1,
        CASE WHEN n%2=0 THEN jsonb_build_object('project_id',$2::text) ELSE '{"project_id":"other"}'::jsonb END
      FROM generate_series(1,6000) n`,[vault,project]);
    const current=await source({project});
    const queue=(await pool.query('INSERT INTO extraction_queue(vault_id,chunk_id) VALUES($1,$2) RETURNING id',[vault,current])).rows[0].id;
    await pool.query('ANALYZE raw_chunks');
    const inspected:Array<{node:string;index?:string;filtered:number}>=[];
    const rows=await db.withTransaction(async client=>freezeExtractionContext(Object.assign(Object.create(client),{
      query:async(sql:string,params:unknown[])=>{
        if(sql.includes('FROM raw_chunks rc')){
          const plan=(await client.query('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+sql,params)).rows[0]['QUERY PLAN'][0].Plan;
          const visit=(node:any)=>{inspected.push({node:node['Node Type'],index:node['Index Name'],filtered:node['Rows Removed by Filter']??0});for(const child of node.Plans??[])visit(child);};visit(plan);
        }
        return client.query(sql,params);
      }
    }),{queueId:queue,vaultId:vault,chunkIds:[current],context:{session_id:'s1',...(project?{project_id:project}:{})},blobStore:'local'}));
    expect(rows.map(r=>r.id)).toEqual([good]);
    // No timing assertion: the regression is reading thousands of ineligible
    // records before returning one, not machine-specific milliseconds.
    expect(inspected.reduce((count,node)=>count+node.filtered,0),JSON.stringify(inspected)).toBeLessThan(32);
  });
});
