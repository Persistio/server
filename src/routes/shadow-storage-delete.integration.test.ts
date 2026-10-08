import crypto from 'node:crypto';
import Fastify from 'fastify';
import { Pool } from 'pg';
import { beforeAll, afterAll, afterEach, describe, it, expect, vi } from 'vitest';
const state=vi.hoisted(()=>({remove:vi.fn(),metric:vi.fn()}));
vi.mock('../services/raw-chunk-storage',()=>({getRawChunkStorage:()=>({store:'local',delete:state.remove})}));
vi.mock('../services/customer-metrics',async original=>({...await original<typeof import('../services/customer-metrics')>(),recordCustomerMetric:state.metric}));
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('shadow storage settlement through real admin route',()=>{
  const pool=new Pool({connectionString:url});const app=Fastify();let db:typeof import('../db/client');
  const vaults:string[]=[];
  beforeAll(async()=>{
    if(!url||process.env.DATABASE_URL!==url||!new URL(url).pathname.startsWith('/persistio_test_'))throw new Error('Isolated test DB required');
    db=await import('../db/client');await db.runMigrations();
    (await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=true;
    await (await import('./admin')).registerAdminRoutes(app);
  });
  afterEach(async()=>{
    vi.useRealTimers();vi.restoreAllMocks();state.metric.mockReset();state.remove.mockReset();
    await pool.query("DELETE FROM customer_metric_shadow_outbox WHERE payload->>'vault_id'=ANY($1::text[])",[vaults]);
    await pool.query('DELETE FROM raw_chunk_blob_deletion_queue WHERE vault_id=ANY($1::uuid[])',[vaults]);vaults.length=0;
  });
  afterAll(async()=>{(await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=false;await app.close();await pool.end();await db?.closePool();});
  async function fixture(){
    const vault=crypto.randomUUID(),workspace=crypto.randomUUID();vaults.push(vault);
    // This is the durable evidence left after the vault itself has been deleted.
    await pool.query(`INSERT INTO raw_chunk_blob_deletion_queue(vault_id,workspace_id,blob_store,blob_key,storage_bytes)
      VALUES($1,$2,'local',$3,123)`,[vault,workspace,'test-shadow/'+vault]);
    return {vault,workspace};
  }
  const request=(vault:string)=>app.inject({method:'DELETE',url:'/admin/vaults/'+vault,headers:{'x-admin-key':'test-admin-key'}});
  it('concurrent blob-delete retries produce one delta, even when legacy telemetry throws',async()=>{
    const f=await fixture();let release!:()=>void;const barrier=new Promise<void>(r=>release=r);let calls=0;
    state.remove.mockImplementation(async()=>{if(++calls===2)release();await barrier;});
    state.metric.mockImplementation(()=>{throw new Error('PRIVATE publisher failure');});
    const responses=await Promise.all([request(f.vault),request(f.vault)]);
    expect(responses.map(r=>r.statusCode)).toEqual([200,200]);
    const rows=(await pool.query("SELECT payload FROM customer_metric_shadow_outbox WHERE payload->>'vault_id'=$1",[f.vault])).rows;
    expect(rows).toHaveLength(1);expect(rows[0].payload).toMatchObject({workspace_id:f.workspace,vault_id:f.vault,operation:'raw_chunk_blob_delete',storage_bytes_delta:-123});
    expect(state.metric).toHaveBeenCalledOnce();
    expect((await request(f.vault)).statusCode).toBe(404);expect(state.remove).toHaveBeenCalledTimes(2);
  });
  it('keeps the same storage deletion envelope when settlement crosses midnight',async()=>{
    const f=await fixture();state.remove.mockResolvedValue(undefined);
    const transact=db.withTransaction;
    vi.useFakeTimers({toFake:['Date']});vi.setSystemTime(new Date('2026-10-06T23:59:59.999Z'));
    vi.spyOn(db,'withTransaction').mockImplementation(work=>transact(async client=>{
      let settlement=false;const query=client.query.bind(client);
      const spy=vi.spyOn(client,'query').mockImplementation((...args:any[])=>{
        if(String(args[0]).includes('AND deleted_at IS NULL RETURNING id'))settlement=true;
        return (query as any)(...args);
      });
      try{const result=await work(client);if(settlement)vi.setSystemTime(new Date('2026-10-07T00:00:00.001Z'));return result;}
      finally{spy.mockRestore();}
    }));
    expect((await request(f.vault)).statusCode).toBe(200);
    const shadow=(await pool.query("SELECT payload FROM customer_metric_shadow_outbox WHERE payload->>'vault_id'=$1",[f.vault])).rows;
    expect(state.metric).toHaveBeenCalledOnce();expect(shadow.map(row=>row.payload)).toEqual(state.metric.mock.calls.map(([event])=>event));
    expect(shadow[0].payload).toMatchObject({event_ts:'2026-10-06T23:59:59.999Z',event_date:'2026-10-06'});
  });
  it('retains the original durable event after a lost deletion settlement acknowledgement',async()=>{
    const f=await fixture();state.remove.mockResolvedValue(undefined);const transact=db.withTransaction;let lost=false;
    vi.spyOn(db,'withTransaction').mockImplementation(async work=>{
      let settlement=false;
      const result=await transact(async client=>{
        const query=client.query.bind(client);
        const spy=vi.spyOn(client,'query').mockImplementation((...args:any[])=>{
          if(String(args[0]).includes('AND deleted_at IS NULL RETURNING id'))settlement=true;
          return (query as any)(...args);
        });
        try{return await work(client);}finally{spy.mockRestore();}
      });
      if(settlement&&!lost){lost=true;throw new Error('Lost COMMIT');}return result;
    });
    expect((await request(f.vault)).statusCode).toBe(500);expect(lost).toBe(true);
    const before=(await pool.query("SELECT payload FROM customer_metric_shadow_outbox WHERE payload->>'vault_id'=$1",[f.vault])).rows;
    expect(before).toHaveLength(1);expect((await request(f.vault)).statusCode).toBe(404);
    expect((await pool.query("SELECT payload FROM customer_metric_shadow_outbox WHERE payload->>'vault_id'=$1",[f.vault])).rows).toEqual(before);
  });
});
