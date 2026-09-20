import {fixtureSource,cleanFixtureSources,fixtureExtractor,fixtureAssessment} from '../../__tests__/arbitration-fixtures';
import crypto from 'node:crypto';
import { Client, Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
/** Real PG17 rows, locks, triggers, queues and scanner. Only the provider and,
 * in date-transition cases, the SQL clock expression are controlled. No query
 * results are fabricated. Run serially on the parent's isolated test database. */
describe.skipIf(!databaseUrl)('future behavioral activation lifecycle (PostgreSQL 17)',()=>{
  const pool=new Pool({connectionString:databaseUrl,options:'-c timezone=Pacific/Honolulu'}),vaults:string[]=[];
  let db:typeof import('../../db/client'),config:ReturnType<typeof import('../../config').getConfig>;
  let scanner:typeof import('../contradiction-scanner');
  let drain:typeof import('../contradiction-activation').drainDueContradictionActivations;
  let vector:string,today:string;
  beforeAll(async()=>{
    const url=new URL(databaseUrl!);
    if(!/(test|temporal|remediation)/i.test(url.pathname))throw Error('Use an isolated test/temporal/remediation database');
    url.searchParams.set('options','-c timezone=Pacific/Honolulu');process.env.DATABASE_URL=url.toString();
    db=await import('../../db/client');await db.runMigrations();
    config=(await import('../../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    scanner=await import('../contradiction-scanner');
    ({drainDueContradictionActivations:drain}=await import('../contradiction-activation'));
    expect((await pool.query('SHOW server_version_num')).rows[0].server_version_num).toMatch(/^17/);
    expect((await pool.query('SHOW TimeZone')).rows[0].TimeZone).toBe('Pacific/Honolulu');
    today=(await pool.query("SELECT (statement_timestamp() AT TIME ZONE 'UTC')::date::text AS day")).rows[0].day;
    const type=(await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector=JSON.stringify([1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)]);
  });
  beforeEach(()=>{
    config.CONTRADICTION_SCAN_ENABLED=true;config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH=1;
    config.CONTRADICTION_SCAN_MIN_SIMILARITY=0.8;
  });
  afterEach(async()=>{
    await cleanFixtureSources();
    vi.restoreAllMocks();
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;
  });
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function vault(){
    const id=crypto.randomUUID();vaults.push(id);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'future-reminders',$2,'unlimited')",[id,crypto.randomUUID()]);return id;
  }
  async function memory(v:string,data:string,from:string|null,extra:{until?:string;type?:string;similarity?:number;id?:string;hash?:string}={}){
    const id=extra.id??crypto.randomUUID(),similarity=extra.similarity??1;
    const embedding=JSON.stringify([similarity,Math.sqrt(1-similarity**2),...JSON.parse(vector).slice(2)]);
    await pool.query(`INSERT INTO memories(id,vault_id,data,subject,hash,embedding,scope,scope_key,status,type,valid_from,valid_until,source_chunks)
      VALUES($1,$2,$3,'Topic',$4,$5::vector,'session','s1','active',$6,$7::date,$8::date,ARRAY[$9::uuid])`,
    [id,v,data,extra.hash??crypto.randomUUID(),embedding,extra.type??'workflow',from,extra.until??null,await fixtureSource(pool,v,data)]);return id;
  }
  const schedule=async(id:string)=>(await pool.query(`SELECT *,revision::text,scan_reference_date::text
    FROM memory_contradiction_schedule WHERE memory_id=$1`,[id])).rows[0];
  const logs=async(v:string)=>(await pool.query('SELECT * FROM contradiction_scan_log WHERE vault_id=$1',[v])).rows;
  const states=async(v:string)=>(await pool.query('SELECT id,status,revision::text FROM memories WHERE vault_id=$1 ORDER BY id',[v])).rows;
  async function only(v:string,id:string){await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[v,id]);}
  async function wake(v:string,id?:string){
    if(id)await pool.query("UPDATE memory_contradiction_schedule SET available_at=now()-interval '1 minute' WHERE memory_id=$1",[id]);
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 minute' WHERE vault_id=$1",[v]);
  }
  const day=(offset:number)=>new Date(Date.parse(today+'T00:00:00Z')+offset*86400000).toISOString().slice(0,10);
  function sqlClock(offset:number,deleteInstead=false){
    const original=Client.prototype.query,connections=new Set<Client>();let negativeControlHits=0;
    const hook=vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      if(typeof args[0]==='string'){
        const sql=args[0];
        if(sql.includes('SELECT memory_id, generation, failures'))connections.add(this);
        if(deleteInstead && sql.includes('SET available_at = $5::date')){
          negativeControlHits++;
          // Exact pre-fix lifecycle: an exhausted scan deletes its reminder.
          args[0]=`DELETE FROM memory_contradiction_schedule WHERE memory_id=$1 AND generation=$2
            AND revision=$3::bigint AND (statement_timestamp() AT TIME ZONE 'UTC')::date=$4::date`;
          args[1]=(args[1] as unknown[]).slice(0,4);
        }
        // Control the reference instant and its persistence fence together. Due
        // selection, schedule timestamps and all returned rows remain real SQL.
        args[0]=(args[0] as string).replaceAll('statement_timestamp()',`TIMESTAMPTZ '${day(offset)} 00:01:00+00'`);
      }
      return Reflect.apply(original,this,args);
    } as any);
    return{hook,connections,hits:()=>negativeControlHits};
  }
  it.each(['user_rule','user_preference','task_pattern','workflow','constraint','decision'])(
    'keeps an SQL-excluded future %s pair, restarts at inclusive valid_from without a revision change, and is idempotent',async type=>{
      const v=await vault(),a=await memory(v,'First rule',today,{type,until:today}),b=await memory(v,'Second rule',today,{type,until:today});
      await only(v,b);const before=await states(v),initial=await schedule(b),extractor=fixtureExtractor(async()=>'keep_both');
      const oldDay=sqlClock(-1);await drain(extractor as never);oldDay.hook.mockRestore();
      expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await logs(v)).toEqual([]);
      expect(await schedule(a)).toBeUndefined(); // distinct hashes: SQL never returned this neighbor
      expect(await schedule(b)).toMatchObject({generation:initial.generation,revision:initial.revision,
        available_at:new Date(today+'T00:00:00Z'),failures:0,scan_memory_id:null,scan_reference_date:null,scan_similarity:null});
      expect(await states(v)).toEqual(before);
      await wake(v);const currentDay=sqlClock(0);await drain(extractor as never);currentDay.hook.mockRestore();
      expect(currentDay.connections.size).toBe(1);
      expect([...currentDay.connections][0]).not.toBe([...oldDay.connections][0]);
      expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();expect(await schedule(b)).toBeUndefined();
      expect(await logs(v)).toHaveLength(1);expect(await states(v)).toEqual(before);
      await wake(v);await drain(extractor as never);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    });
  it('negative control: restoring old completion deletion loses the unchanged pair at activation',async()=>{
    const v=await vault();await memory(v,'First rule',today);const b=await memory(v,'Second rule',today);
    await only(v,b);const before=await states(v),extractor={arbitrateConflict:vi.fn()};
    const control=sqlClock(-1,true);await drain(extractor as never);control.hook.mockRestore();
    expect(control.hits()).toBe(1);expect(await schedule(b)).toBeUndefined();
    await wake(v);await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await logs(v)).toEqual([]);expect(await states(v)).toEqual(before);
  });
  it('expires a retained same-day behavior without model cost when restart misses its window',async()=>{
    const v=await vault();await memory(v,'First rule',today,{until:today});const b=await memory(v,'Second rule',today,{until:today});
    await only(v,b);const extractor={arbitrateConflict:vi.fn()};
    const future=sqlClock(-1);await drain(extractor as never);future.hook.mockRestore();expect(await schedule(b)).toBeDefined();
    await wake(v);const expired=sqlClock(1);await drain(extractor as never);expired.hook.mockRestore();
    expect(await schedule(b)).toBeUndefined();expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await logs(v)).toEqual([]);
  });
  it('discovers every future neighbor date in bounded pages from an unbounded current memory, keeping the earliest pending wakeup',async()=>{
    const v=await vault(),b=await memory(v,'Unbounded workflow',null);
    const later=await memory(v,'Later workflow','2099-03-01',{similarity:0.99});
    const earliest=await memory(v,'Earlier but lower-ranked workflow','2099-01-01',{similarity:0.95});
    const middle=await memory(v,'Middle and lowest-ranked workflow','2099-02-01',{similarity:0.9});
    await only(v,b);const extractor={arbitrateConflict:vi.fn()};
    for(let pass=0;pass<3;pass++){await wake(v,b);await drain(extractor as never);}
    expect(await schedule(b)).toBeUndefined();expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    for(const [id,date] of [[later,'2099-03-01'],[earliest,'2099-01-01'],[middle,'2099-02-01']]){
      expect(await schedule(id)).toMatchObject({available_at:new Date(date+'T00:00:00Z'),scan_memory_id:null});
    }
    expect((await pool.query('SELECT pending_count::int,next_visit_at FROM memory_contradiction_pending_vaults WHERE vault_id=$1',[v])).rows)
      .toEqual([{pending_count:3,next_visit_at:new Date('2099-01-01T00:00:00Z')}]);
    // Unlike windows retain both without claiming a successful model verdict.
    expect((await logs(v)).map(r=>r.decision)).toEqual(['incomplete','incomplete','incomplete']);
  });
  it('does not overwrite a discovered neighbor\'s due time, generation, failures or retained cursor',async()=>{
    const v=await vault(),b=await memory(v,'Unbounded workflow',null),a=await memory(v,'Future workflow','2099-01-01');
    await pool.query(`UPDATE memory_contradiction_schedule SET available_at=now()-interval '1 hour',failures=3,
      scan_reference_date=(statement_timestamp() AT TIME ZONE 'UTC')::date,scan_similarity=0.9,scan_memory_id=$2 WHERE memory_id=$1`,[a,b]);
    const before=await schedule(a);
    await scanner.scanForContradictions(v,[b],{arbitrateConflict:vi.fn()} as never);
    expect(await schedule(a)).toEqual(before);
  });
  it.each(['type','window','archive','delete'])('does not resurrect a future neighbor after a concurrent %s change',async change=>{
    const v=await vault(),b=await memory(v,'Unbounded workflow',null),a=await memory(v,'Future workflow','2099-01-01');
    await only(v,b);const original=db.query;let raced=false;
    vi.spyOn(db,'query').mockImplementation(async(sql:string,params?:any[])=>{
      if(!raced && sql.startsWith('WITH future_memories')){
        raced=true;
        if(change==='type')await pool.query("UPDATE memories SET type='system_fact' WHERE id=$1",[a]);
        if(change==='window')await pool.query('UPDATE memories SET valid_from=NULL WHERE id=$1',[a]);
        if(change==='archive')await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[a]);
        if(change==='delete')await pool.query('DELETE FROM memories WHERE id=$1',[a]);
      }
      return original(sql,params);
    });
    // Arbitration's later revision lock also rejects the changed candidate.
    await expect(scanner.scanForContradictions(v,[b],{arbitrateConflict:vi.fn()} as never)).rejects.toThrow('inputs changed');
    expect(raced).toBe(true);
    if(change==='archive'||change==='delete')expect(await schedule(a)).toBeUndefined();
    else expect(await schedule(a)).toMatchObject({revision:'2',scan_memory_id:null});
    expect(await logs(v)).toEqual([]);
  });
  it.each(['generation','midnight'])('fences future rescheduling across a concurrent %s change',async change=>{
    const v=await vault(),b=await memory(v,'Future workflow','2099-01-01');
    const initial=await schedule(b),original=Client.prototype.query;let raced=false;
    vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      if(!raced && typeof args[0]==='string' && args[0].includes('SET available_at = $5::date')){
        raced=true;
        if(change==='generation')return pool.query("UPDATE memories SET valid_from='2099-02-01' WHERE id=$1",[b])
          .then(()=>Reflect.apply(original,this,args));
        args[0]=args[0].replaceAll('statement_timestamp()',"(statement_timestamp()+interval '1 day')");
      }
      return Reflect.apply(original,this,args);
    } as any);
    await drain({arbitrateConflict:vi.fn()} as never);expect(raced).toBe(true);
    const after=await schedule(b);
    expect(after.available_at.getTime()).toBeLessThan(Date.now()+1000);
    if(change==='generation')expect(after.generation).not.toBe(initial.generation);
    else expect(after.generation).toBe(initial.generation);
    expect(after.scan_memory_id).toBeNull();expect(await logs(v)).toEqual([]);
  });
  it('does not log a pair or lose its due reminder when connection loss interrupts neighbor preservation',async()=>{
    const v=await vault(),b=await memory(v,'Unbounded workflow',null);await memory(v,'Future workflow','2099-01-01');
    await only(v,b);const original=Client.prototype.query;let interrupted=false;
    const hook=vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      if(!interrupted && typeof args[0]==='string' && args[0].startsWith('WITH future_memories')){
        interrupted=true;
        return pool.query(`SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity
          WHERE datname=current_database() AND application_name='persistio:contradiction-activation'`)
          .then(result=>{expect(result.rows).toEqual([{stopped:true}]);return Reflect.apply(original,this,args);});
      }
      return Reflect.apply(original,this,args);
    } as any);
    await expect(drain({arbitrateConflict:vi.fn()} as never)).rejects.toThrow();hook.mockRestore();
    expect(interrupted).toBe(true);expect((await schedule(b)).scan_memory_id).toBeNull();expect(await logs(v)).toEqual([]);
    await wake(v,b);await drain({arbitrateConflict:vi.fn()} as never);
    expect(await schedule(b)).toBeUndefined();expect(await logs(v)).toHaveLength(1);
  });
});
