import crypto from 'node:crypto';
import {Pool} from 'pg';
import {beforeAll,afterAll,afterEach,describe,it,expect,vi} from 'vitest';
const storage=vi.hoisted(()=>({get:vi.fn()}));
vi.mock('../raw-chunk-storage',()=>({getRawChunkStorage:()=>({store:'local',get:storage.get})}));
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('explicit technical memory-job recovery (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:url}),vaults:string[]=[],receipts:string[]=[];
  let recovery:typeof import('../memory-job-recovery'),db:typeof import('../../db/client');
  beforeAll(async()=>{process.env.DATABASE_URL=url;db=await import('../../db/client');await db.runMigrations();
    (await import('../../config')).getConfig().ENCRYPTION_ENABLED=false;recovery=await import('../memory-job-recovery');});
  afterEach(async()=>{storage.get.mockReset();await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])',[receipts]);
    await pool.query('DELETE FROM plans WHERE id=ANY($1::text[])',[vaults.map(v=>'baseline-'+v)]);vaults.length=0;receipts.length=0;});
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function fixture(kind:'extraction'|'curation',receipt=true){
    const vault=crypto.randomUUID(),queue=crypto.randomUUID(),failure=crypto.randomUUID(),source=crypto.randomUUID();vaults.push(vault);receipts.push(queue);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'recovery-test',$2,'unlimited')",[vault,crypto.randomUUID()]);
    let memory:string|undefined;
    if(kind==='extraction'){
      await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_key,blob_store,storage_bytes) VALUES($1,$2,'session','user',$3,'local',20)",[source,vault,'vaults/'+vault+'/'+source]);
      await pool.query(`INSERT INTO extraction_dead_letter(id,vault_id,chunk_id,source_queue_id,retry_count,last_error,context_chunk_ids)
        VALUES($1,$2,$3,$4,5,'PRIVATE failure sentinel','{}')`,[failure,vault,source,queue]);
      storage.get.mockResolvedValue('A supported durable fact');
    }else{
      memory=crypto.randomUUID();
      await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,status,scope,scope_key) VALUES($1,$2,'Durable fact','Topic',$3,'active','session','session')",[memory,vault,crypto.randomUUID()]);
      await pool.query(`INSERT INTO curation_dead_letter(id,vault_id,source_queue_id,retry_count,last_error,targets)
        VALUES($1,$2,$3,5,'PRIVATE failure sentinel',$4::jsonb)`,[failure,vault,queue,JSON.stringify([{memory_id:memory,revision:'1'}])]);
    }
    if(receipt)await pool.query("INSERT INTO worker_action_receipts(queue_kind,queue_id,action_key,claim_token) VALUES($1,$2,'dead-letter',$3)",[kind,queue,crypto.randomUUID()]);
    return{vault,queue,failure,source,memory,input:{kind,vaultId:vault,failureId:failure,actorId:'test-operator',reason:'Synthetic explicit retry'}};
  }
  it.each(['extraction','curation'] as const)('enqueues one %s retry under concurrent repeated invocation and preserves failure evidence',async kind=>{
    const f=await fixture(kind);const results=await Promise.all([recovery.retryMemoryJobFailure(f.input),recovery.retryMemoryJobFailure(f.input)]);
    expect(new Set(results.map(r=>r.queueId)).size).toBe(1);expect(results.filter(r=>r.alreadyRetried)).toHaveLength(1);
    expect((await pool.query('SELECT queue_id,actor_id,reason FROM memory_job_recoveries WHERE failure_id=$1',[f.failure])).rows)
      .toEqual([{queue_id:results[0].queueId,actor_id:'test-operator',reason:'Synthetic explicit retry'}]);
    await expect(pool.query("UPDATE memory_job_recoveries SET reason='changed' WHERE failure_id=$1",[f.failure])).rejects.toThrow('append-only');
    const listed=await recovery.listMemoryJobFailures(f.vault);expect(JSON.stringify(listed)).not.toContain('PRIVATE');expect(listed[0]).toMatchObject({id:f.failure,recovery_queue_id:results[0].queueId});
    await pool.query(`DELETE FROM ${kind}_queue WHERE id=$1`,[results[0].queueId]);
    expect(await recovery.retryMemoryJobFailure(f.input)).toEqual({queueId:results[0].queueId,alreadyRetried:true});
  });
  it('refuses missing objects, missing receipts and cross-vault requests without enqueueing',async()=>{
    const f=await fixture('extraction');storage.get.mockRejectedValueOnce(new Error('PRIVATE object error'));
    await expect(recovery.retryMemoryJobFailure(f.input)).rejects.toThrow('missing or corrupt');
    await expect(recovery.retryMemoryJobFailure({...f.input,vaultId:crypto.randomUUID()})).rejects.toThrow('no recoverable worker receipt');
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=$1',[f.queue]);
    await expect(recovery.retryMemoryJobFailure(f.input)).rejects.toThrow('completed dead-letter');
    expect((await pool.query('SELECT id FROM extraction_queue WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
  });
  it('refuses extraction work already represented by another queued segment',async()=>{
    const f=await fixture('extraction');const segment=crypto.randomUUID();
    await pool.query("INSERT INTO segments(id,vault_id,session_id,chunk_ids) VALUES($1,$2,'session',$3::uuid[])",[segment,f.vault,[f.source]]);
    await pool.query('INSERT INTO extraction_queue(vault_id,segment_id) VALUES($1,$2)',[f.vault,segment]);
    await expect(recovery.retryMemoryJobFailure(f.input)).rejects.toThrow('queued or claimed work');
  });
  it.each(['extraction','curation'] as const)('cannot resurrect deleted %s inputs from retained failure/receipt history',async kind=>{
    const f=await fixture(kind);
    if(kind==='extraction')await pool.query('DELETE FROM raw_chunks WHERE id=$1',[f.source]);
    else await pool.query('DELETE FROM memories WHERE id=$1',[f.memory]);
    await expect(recovery.retryMemoryJobFailure(f.input)).rejects.toThrow();
    expect((await pool.query(`SELECT id FROM ${kind}_queue WHERE vault_id=$1`,[f.vault])).rowCount).toBe(0);
    expect((await pool.query(`SELECT id FROM ${kind}_dead_letter WHERE id=$1`,[f.failure])).rowCount).toBe(1);
    expect((await pool.query('SELECT queue_id FROM worker_action_receipts WHERE queue_id=$1',[f.queue])).rowCount).toBe(1);
  });
  it.each(['revision','archived','plan','queued'] as const)('refuses curation retry after %s changes without hiding the memory',async change=>{
    const f=await fixture('curation');
    if(change==='revision')await pool.query("UPDATE memories SET data='Independent correction' WHERE id=$1",[f.memory]);
    if(change==='archived')await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[f.memory]);
    if(change==='plan'){
      await pool.query('INSERT INTO plans(id,limits) VALUES($1,$2::jsonb)',['baseline-'+f.vault,JSON.stringify({curator_enabled:false})]);
      await pool.query('UPDATE vaults SET plan_id=$2 WHERE id=$1',[f.vault,'baseline-'+f.vault]);
    }
    if(change==='queued')await db.withTransaction(async client=>{const {enqueueCurationWork}=await import('../curation-work');await enqueueCurationWork(client,{vaultId:f.vault,workKey:'independent',memoryIds:[f.memory!]});});
    await expect(recovery.retryMemoryJobFailure(f.input)).rejects.toThrow();
    expect((await pool.query('SELECT status FROM memories WHERE id=$1',[f.memory])).rows[0].status).toBe('active');
    expect((await pool.query('SELECT queue_id FROM memory_job_recoveries WHERE failure_id=$1',[f.failure])).rowCount).toBe(0);
  });
});
