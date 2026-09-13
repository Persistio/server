import crypto from 'node:crypto';
import { Client, Pool } from 'pg';
import { afterAll,afterEach,beforeAll,beforeEach,describe,it,expect,vi } from 'vitest';
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('revision-tagged contradiction activation (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl}),vaults:string[]=[];
  let db:typeof import('../../db/client'),config:ReturnType<typeof import('../../config').getConfig>;
  let drain:typeof import('../contradiction-activation').drainDueContradictionActivations;
  let scan:typeof import('../contradiction-scanner').scanForContradictions;
  let vector:number[];
  beforeAll(async()=>{
    process.env.DATABASE_URL=databaseUrl;db=await import('../../db/client');await db.runMigrations();
    config=(await import('../../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    ({drainDueContradictionActivations:drain}=await import('../contradiction-activation'));
    ({scanForContradictions:scan}=await import('../contradiction-scanner'));
    const type=(await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector=[1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)];
  });
  beforeEach(()=>{config.CONTRADICTION_SCAN_ENABLED=true;config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH=4;});
  afterEach(async()=>{
    vi.restoreAllMocks();await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;
  });
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function vault(){
    const id=crypto.randomUUID();vaults.push(id);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id,rate_limit_override) VALUES($1,'conflict-restoration',$2,'unlimited','{\"curator_enabled\":false}')",[id,crypto.randomUUID()]);
    return id;
  }
  async function memory(vaultId:string,data:string,extra:{scope?:string;scopeKey?:string|null;from?:string|null;until?:string|null;type?:string}={}){
    const id=crypto.randomUUID(),scope=extra.scope??'session';
    await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,embedding,scope,scope_key,status,type,valid_from,valid_until) VALUES($1,$2,$3,'Topic',$4,$5::vector,$6,$7,'active',$8,$9::date,$10::date)",
      [id,vaultId,data,crypto.randomUUID(),JSON.stringify(vector),scope,scope==='global'?null:extra.scopeKey??'s1',extra.type??'system_fact',extra.from??null,extra.until??null]);
    await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)',[id,JSON.stringify(vector)]);
    return id;
  }
  const schedule=async(id:string)=>(await pool.query('SELECT *,revision::text AS revision FROM memory_contradiction_schedule WHERE memory_id=$1',[id])).rows[0];
  async function due(id:string){
    await pool.query("UPDATE memory_contradiction_schedule SET available_at=now()-interval '1 second' WHERE vault_id=$1",[id]);
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 second' WHERE vault_id=$1",[id]);
  }
  async function pair(){
    const v=await vault(),old=await memory(v,'The service uses PostgreSQL.'),current=await memory(v,'The service uses SQLite.');
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[v,current]);
    return{v,old,current};
  }
  const statuses=async(v:string)=>(await pool.query('SELECT id,status,archived_at FROM memories WHERE vault_id=$1',[v])).rows;
  it.each(['global','session','task','project'])('schedules %s memories once, without any approval state',async scope=>{
    const v=await vault(),id=await memory(v,'Durable fact',{scope});
    const rows=(await pool.query('SELECT * FROM memory_contradiction_schedule WHERE memory_id=$1',[id])).rows;
    expect(rows).toHaveLength(1);expect(rows[0]).not.toHaveProperty('policy');expect(rows[0]).not.toHaveProperty('authority_ready');
    expect((await pool.query('SELECT pending_count::int FROM memory_contradiction_pending_vaults WHERE vault_id=$1',[v])).rows).toEqual([{pending_count:1}]);
  });
  it.each([{from:'2099-01-01'},{until:'2020-01-01'}])('scans historical/future knowledge without waiting for applicability: %j',async dates=>{
    const v=await vault(),old=await memory(v,'Older observation',dates),current=await memory(v,'Different observation',dates);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[v,current]);
    const extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
    await drain(extractor as never);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(await schedule(current)).toBeUndefined();expect((await statuses(v)).every(r=>r.status==='active')).toBe(true);
    expect((await pool.query('SELECT decision FROM contradiction_scan_log WHERE vault_id=$1 AND memory_id_a=$2',[v,old])).rows).toEqual([{decision:'keep_both'}]);
  });
  it('uses database due time even when the application clock is ahead or behind',async()=>{
    const f=await pair(),realNow=Date.now;
    vi.spyOn(Date,'now').mockImplementation(()=>realNow()-86400000);
    await drain({arbitrateConflict:async()=>'keep_both'} as never);expect(await schedule(f.current)).toBeUndefined();
    const next=await memory(f.v,'Another observation');
    await pool.query("UPDATE memory_contradiction_schedule SET available_at=now()+interval '1 day' WHERE memory_id=$1",[next]);
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()+interval '1 day' WHERE vault_id=$1",[f.v]);
    vi.spyOn(Date,'now').mockImplementation(()=>realNow()+2*86400000);
    const extractor={arbitrateConflict:vi.fn()};await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await schedule(next)).toBeDefined();
  });
  it('retains disabled work and resumes without an approval workflow',async()=>{
    const f=await pair(),extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
    config.CONTRADICTION_SCAN_ENABLED=false;await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await schedule(f.current)).toBeDefined();
    config.CONTRADICTION_SCAN_ENABLED=true;await drain(extractor as never);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });
  it('preserves generations and backoff on provider failure without hiding memories',async()=>{
    const f=await pair(),before=await schedule(f.current);
    await drain({arbitrateConflict:async()=>{throw Error('Provider unavailable');}} as never);
    const after=await schedule(f.current);
    expect(after.generation).toBe(before.generation);expect(after.failures).toBe(1);expect(after.available_at.getTime()).toBeGreaterThan(Date.now());
    expect((await statuses(f.v)).every(r=>r.status==='active')).toBe(true);
    expect((await pool.query('SELECT id FROM contradiction_scan_log WHERE vault_id=$1',[f.v])).rowCount).toBe(0);
  });
  it('shares one provider budget across vaults and resumes partial scans',async()=>{
    const first=await pair(),second=await pair();await memory(first.v,'A third conflicting observation');
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[first.v,first.current]);
    config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH=1;
    const extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
    for(let i=0;i<3;i++){
      await due(first.v);await due(second.v);
      const before=extractor.arbitrateConflict.mock.calls.length;
      await drain(extractor as never);expect(extractor.arbitrateConflict.mock.calls.length-before).toBeLessThanOrEqual(1);
    }
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(3);
    expect(await schedule(first.current)).toBeUndefined();expect(await schedule(second.current)).toBeUndefined();
  });
  it('skips a locked vault, gives another vault a turn, and releases ownership',async()=>{
    const f=await pair(),other=await pair(),owner=new Client({connectionString:databaseUrl});await owner.connect();
    const key='persistio:contradiction-activation:'+f.v;
    await owner.query('SELECT pg_advisory_lock(hashtextextended($1,0))',[key]);
    const extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
    try{
      await drain(extractor as never);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
      expect(await schedule(other.current)).toBeUndefined();expect(await schedule(f.current)).toBeDefined();
      await owner.query('SELECT pg_advisory_unlock(hashtextextended($1,0))',[key]);await due(f.v);
      await drain(extractor as never);expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(2);
      expect((await owner.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS locked',[key])).rows[0].locked).toBe(true);
    }finally{await owner.end();}
  });
  it('refreshes substantive generations, ignores recall/no-op updates, and removes inactive work',async()=>{
    const f=await pair(),before=await schedule(f.current);
    await pool.query('UPDATE memories SET recall_count=recall_count+1,last_recalled=now(),updated_at=now() WHERE id=$1',[f.current]);
    expect((await schedule(f.current)).generation).toBe(before.generation);
    await pool.query("UPDATE memories SET valid_until='2020-01-01' WHERE id=$1",[f.current]);
    const revised=await schedule(f.current);expect(revised.generation).not.toBe(before.generation);expect(revised.revision).toBe('2');
    expect((await pool.query('DELETE FROM memory_contradiction_schedule WHERE memory_id=$1 AND generation=$2',[f.current,before.generation])).rowCount).toBe(0);
    await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[f.current]);expect(await schedule(f.current)).toBeUndefined();
    await pool.query('UPDATE memories SET archived_at=NULL WHERE id=$1',[f.current]);expect(await schedule(f.current)).toBeDefined();
    await pool.query('DELETE FROM memories WHERE id=$1',[f.current]);expect(await schedule(f.current)).toBeUndefined();
  });
  it('preserves a concurrent enqueue while advancing the pending-vault visit',async()=>{
    const v=await vault();await memory(v,'Initial observation');
    const original=Client.prototype.query;let added:string|undefined;
    vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      const result=Reflect.apply(original,this,args);
      if(!added && typeof args[0]==='string' && args[0].includes('SELECT available_at FROM memory_contradiction_schedule')){
        return Promise.resolve(result).then(async rows=>{added=await memory(v,'New observation after snapshot');return rows;});
      }
      return result;
    } as any);
    await drain({arbitrateConflict:vi.fn()} as never);expect(added).toBeDefined();
    expect((await pool.query('SELECT pending_count::int,next_visit_at<=now() AS ready FROM memory_contradiction_pending_vaults WHERE vault_id=$1',[v])).rows)
      .toEqual([{pending_count:1,ready:true}]);
  });
  it('cannot apply a decision after its advisory-lock connection is lost',async()=>{
    const f=await pair();
    const extractor={arbitrateConflict:vi.fn(async()=>{
      const terminated=await pool.query("SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity WHERE datname=current_database() AND application_name='persistio:contradiction-activation'");
      expect(terminated.rows).toEqual([{stopped:true}]);return 'supersede_old';
    })};
    await expect(drain(extractor as never)).rejects.toThrow();
    expect((await statuses(f.v)).every(r=>r.status==='active')).toBe(true);
    expect((await pool.query('SELECT id FROM contradiction_scan_log WHERE vault_id=$1',[f.v])).rowCount).toBe(0);
    expect(await schedule(f.current)).toBeDefined();
    await drain({arbitrateConflict:async()=>'keep_both'} as never);expect(await schedule(f.current)).toBeUndefined();
  });
  it.each(['scope','dates','data','archive','delete','encryption'])('rejects stale full decisions after concurrent %s changes',async change=>{
    const f=await pair();
    await drain({arbitrateConflict:async()=>{
      if(change==='scope')await pool.query("UPDATE memories SET scope_key='s2' WHERE id=$1",[f.old]);
      if(change==='dates')await pool.query("UPDATE memories SET valid_until='2020-01-01' WHERE id=$1",[f.old]);
      if(change==='data')await pool.query("UPDATE memories SET data='Independent correction' WHERE id=$1",[f.old]);
      if(change==='archive')await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[f.old]);
      if(change==='delete')await pool.query('DELETE FROM memories WHERE id=$1',[f.old]);
      if(change==='encryption')await pool.query('UPDATE vaults SET vault_encryption_enabled=true WHERE id=$1',[f.v]);
      return 'supersede_old';
    }} as never);
    expect((await statuses(f.v)).every(r=>r.status==='active')).toBe(true);
    expect((await pool.query('SELECT id FROM contradiction_scan_log WHERE vault_id=$1',[f.v])).rowCount).toBe(0);
    expect((await schedule(f.current)).failures).toBe(1);
  });
  it.each(['scope','type','dates'])('keeps unlike %s memories separate even with identical text',async change=>{
    const v=await vault(),a=await memory(v,'An identical observation'),b=await memory(v,'An identical observation',
      change==='scope'?{scopeKey:'s2'}:change==='type'?{type:'decision'}:{until:'2020-12-31'});
    const extractor={arbitrateConflict:vi.fn(async()=>'merge')};await scan(v,[b],extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect((await statuses(v)).every(r=>r.status==='active')).toBe(true);
    expect(await schedule(a)).toBeDefined();
  });
  it('records a merge atomically with source union and no confidence inflation',async()=>{
    const v=await vault(),a=await memory(v,'Same observation'),b=await memory(v,'Same observation'),sources=[crypto.randomUUID(),crypto.randomUUID()];
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role) SELECT id,$1,'s1','user' FROM unnest($2::uuid[]) id",[v,sources]);
    await pool.query('UPDATE memories SET source_chunks=ARRAY[$2::uuid],confidence=0.8 WHERE id=$1',[a,sources[0]]);
    await pool.query("UPDATE memories SET source_chunks=ARRAY[$2::uuid],sensitivity='high',confidence=0.9 WHERE id=$1",[b,sources[1]]);
    await scan(v,[b],{arbitrateConflict:vi.fn()} as never);
    const survivor=(await pool.query('SELECT source_chunks,sensitivity,confidence FROM memories WHERE id=$1',[a])).rows[0];
    expect(survivor.source_chunks.sort()).toEqual(sources.sort());expect(survivor.sensitivity).toBe('high');expect(survivor.confidence).toBe(0.8);
    expect((await statuses(v)).find(r=>r.id===b)).toMatchObject({status:'superseded'});
  });
});
