import {fixtureSource,cleanFixtureSources,fixtureExtractor,fixtureAssessment} from '../../__tests__/arbitration-fixtures';
import crypto from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { Client, Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const kms=vi.hoisted(()=>({unwrap:vi.fn()}));
vi.mock('@google-cloud/kms',()=>({KeyManagementServiceClient:class {decrypt=kms.unwrap;}}));
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('durable bounded contradiction passes (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl,options:'-c timezone=Pacific/Honolulu'}),vaults:string[]=[];
  const dek=Buffer.alloc(32,23);
  let db:typeof import('../../db/client'),crypt:typeof import('../crypto');
  let config:ReturnType<typeof import('../../config').getConfig>;
  let scanner:typeof import('../contradiction-scanner');
  let drain:typeof import('../contradiction-activation').drainDueContradictionActivations;
  let vector:string;
  beforeAll(async()=>{
    const url=new URL(databaseUrl!);
    url.searchParams.set('options','-c timezone=Pacific/Honolulu');
    process.env.DATABASE_URL=url.toString();
    db=await import('../../db/client');await db.runMigrations();
    config=(await import('../../config')).getConfig();
    config.ENCRYPTION_ENABLED=true;config.KEY_PROVIDER='gcp_kms';
    config.GCP_KMS_KEY_NAME='projects/test/locations/global/keyRings/test/cryptoKeys/test';
    crypt=await import('../crypto');await crypt.initCryptoClient();
    scanner=await import('../contradiction-scanner');
    ({drainDueContradictionActivations:drain}=await import('../contradiction-activation'));
    const type=(await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector=JSON.stringify([1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)]);
    expect((await pool.query('SHOW server_version_num')).rows[0].server_version_num).toMatch(/^17/);
    expect((await pool.query('SHOW TimeZone')).rows[0].TimeZone).toBe('Pacific/Honolulu');
  });
  beforeEach(()=>{
    config.CONTRADICTION_SCAN_ENABLED=true;config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH=1;
    kms.unwrap.mockResolvedValue([{plaintext:dek}]);
  });
  afterEach(async()=>{
    await cleanFixtureSources();
    vi.restoreAllMocks();kms.unwrap.mockReset();
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;
  });
  afterAll(async()=>{await pool.end();await db?.closePool();});
  const id=(n:number)=>`10000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
  async function fixture(encrypted=false,count=scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS*2+3,exact=true){
    const vaultId=crypto.randomUUID();vaults.push(vaultId);
    await pool.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id,encrypted_dek,vault_encryption_enabled)
      VALUES($1,'bounded-scanner',$2,'unlimited',$3,$4)`,[vaultId,crypto.randomUUID(),encrypted?'test-wrapped-dek':null,encrypted]);
    // Unique UUID prefix per fixture, monotonically ordered suffix for tie breaks.
    const prefix=crypto.randomUUID().slice(0,24),key=(n:number)=>prefix+id(n).slice(24);
    const data='Use this future workflow.';
    const hash=crypto.createHash('sha256').update(data).digest('hex');
    const rows=Array.from({length:count},(_,i)=>({id:key(i+1),data:`False hash hint ${i}`}));
    const duplicate=exact?key(count+1):null,current=key(count+2);
    if(duplicate)rows.push({id:duplicate,data});rows.push({id:current,data});
    await pool.query(`INSERT INTO memories(id,vault_id,data,subject,hash,embedding,scope,scope_key,status,type,valid_from,source_chunks)
      SELECT r.id,$1,r.data,'Topic',$2,$3::vector,'session','s1','active','workflow',DATE '2099-01-01',ARRAY[$5::uuid]
      FROM jsonb_to_recordset($4::jsonb) AS r(id uuid,data text)`,
    [vaultId,hash,vector,JSON.stringify(rows.map(r=>({...r,data:encrypted?crypt.encryptField(r.data,dek):r.data}))),await fixtureSource(pool,vaultId,data)]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[vaultId,current]);
    return{vaultId,current,duplicate,keys:rows.map(r=>r.id),data};
  }
  const schedule=async(memoryId:string)=>(await pool.query(`SELECT *,revision::text,scan_reference_date::text
    FROM memory_contradiction_schedule WHERE memory_id=$1`,[memoryId])).rows[0];
  const logs=async(vaultId:string)=>(await pool.query('SELECT memory_id_a,memory_id_b,decision FROM contradiction_scan_log WHERE vault_id=$1',[vaultId])).rows;
  async function due(vaultId:string){
    await pool.query("UPDATE memory_contradiction_schedule SET available_at=now()-interval '1 minute' WHERE vault_id=$1 AND available_at < now()+interval '1 day'",[vaultId]);
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 minute' WHERE vault_id=$1",[vaultId]);
  }
  function observeReads(){
    const original=Client.prototype.query;
    const reads:Array<{vault:string;key:string|null;count:number;limit:number;current:boolean}>=[];
    const connections=new Set<Client>();
    vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      const sql=args[0],params=args[1] as any[];
      const result=Reflect.apply(original,this,args);
      if(typeof sql==='string' && (sql.includes('FROM memories original') || sql.includes('1.0 AS similarity'))){
        const first=!connections.has(this);
        connections.add(this);
        return Promise.resolve(result).then(async rows=>{
          if(first)expect((await Reflect.apply(original,this,['SHOW TimeZone'])).rows[0].TimeZone).toBe('Pacific/Honolulu');
          reads.push({vault:params[0],key:params[7]??null,count:rows.rows.length,limit:params[4]??1,current:sql.includes('1.0 AS similarity')});
          return rows;
        });
      }
      return result;
    } as any);
    return{reads,connections};
  }
  it.each([false,true])('yields the lock after <=cap reads/decryptions, resumes on new connections, and retains future equal-text alternatives (encrypted=%s)',async encrypted=>{
    const f=await fixture(encrypted),other=await fixture(false,1,false);
    // A factual second vault consumes a real model turn while the first only skips hints.
    await pool.query("UPDATE memories SET type='system_fact' WHERE vault_id=$1",[other.vaultId]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[other.vaultId,other.current]);
    await due(f.vaultId);
    const before=await schedule(f.current),extractor=fixtureExtractor(async()=>'keep_both' as const);
    const observed=observeReads(),decrypt=vi.spyOn(crypt,'decryptForVault');
    await drain(extractor as never);
    const cursor=await schedule(f.current);
    expect(cursor).toMatchObject({generation:before.generation,revision:before.revision,failures:0,
      scan_memory_id:f.keys[scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS-2],scan_similarity:1});
    expect(cursor.scan_reference_date).toBe((await pool.query("SELECT (statement_timestamp() AT TIME ZONE 'UTC')::date::text AS day")).rows[0].day);
    expect(cursor.available_at.getTime()).toBeGreaterThan(Date.now());
    expect(observed.reads.filter(r=>r.vault===f.vaultId).reduce((n,r)=>n+r.count,0)).toBe(scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    expect(decrypt.mock.calls.filter(([v])=>v.id===f.vaultId)).toHaveLength(scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    expect(await logs(f.vaultId)).toEqual([]);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(await schedule(other.current)).toBeUndefined();
    const lock=new Client({connectionString:databaseUrl});await lock.connect();
    try{expect((await lock.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS locked',[
      'persistio:contradiction-activation:'+f.vaultId])).rows[0].locked).toBe(true);}finally{await lock.end();}
    for(let pass=0;pass<2;pass++){
      const previous=await schedule(f.current),start=observed.reads.length,decryptStart=decrypt.mock.calls.length;
      await due(f.vaultId);await drain(extractor as never);
      const reads=observed.reads.slice(start).filter(r=>r.vault===f.vaultId);
      expect(reads.find(r=>!r.current)?.key).toBe(previous.scan_memory_id);
      expect(reads.reduce((n,r)=>n+r.count,0)).toBeLessThanOrEqual(scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS);
      expect(decrypt.mock.calls.length-decryptStart).toBeLessThanOrEqual(scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    }
    expect(observed.connections.size).toBe(3);expect(await schedule(f.current)).toMatchObject({available_at:new Date('2099-01-01T00:00:00Z'),scan_memory_id:null});
    expect(await logs(f.vaultId)).toEqual([]);
    expect((await pool.query('SELECT status,count(*)::int AS n FROM memories WHERE vault_id=$1 GROUP BY status',[f.vaultId])).rows)
      .toEqual([{status:'active',n:f.keys.length}]);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });
  it('bounds and resumes historical unresolved neighbors outside similarity without treating history as authority',async()=>{
    const f=await fixture(false,scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS+3,false);
    await pool.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b)
      SELECT $1,m.id,current.id,'keep_both',1,m.revision,current.revision FROM memories m,memories current
      WHERE m.vault_id=$1 AND current.id=$2 AND m.id<>current.id`,[f.vaultId,f.current]);
    const originalVector=JSON.parse(vector),orthogonal=[0,1,...originalVector.slice(2)];
    await pool.query('UPDATE memories SET embedding=$2::vector WHERE id=$1',[f.current,JSON.stringify(orthogonal)]);
    const observed=observeReads(),extractor={arbitrateConflict:vi.fn()};
    await drain(extractor as never);
    expect(observed.reads.reduce((n,r)=>n+r.count,0)).toBe(scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    const cursor=await schedule(f.current);expect(cursor.scan_similarity).toBe(0);expect(cursor.scan_memory_id).not.toBeNull();
    await due(f.vaultId);await drain(extractor as never);
    expect(await schedule(f.current)).toMatchObject({available_at:new Date('2099-01-01T00:00:00Z'),scan_memory_id:null});
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await logs(f.vaultId)).toHaveLength(f.keys.length-1);
  });
  it('terminates after a full-cap last page without fabricating decisions',async()=>{
    const f=await fixture(true,scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS-1,false),extractor={arbitrateConflict:vi.fn()};
    await drain(extractor as never);expect((await schedule(f.current)).scan_memory_id).toBe(f.keys.at(-2));
    await due(f.vaultId);await drain(extractor as never);
    expect(await schedule(f.current)).toMatchObject({available_at:new Date('2099-01-01T00:00:00Z'),scan_memory_id:null});expect(await logs(f.vaultId)).toEqual([]);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
  });
  it('resets progress only on substantive generation/revision changes, not counter-only writes',async()=>{
    const f=await fixture();await drain({arbitrateConflict:vi.fn()} as never);
    const before=await schedule(f.current);
    await pool.query('UPDATE memories SET recall_count=recall_count+1,last_recalled=now(),updated_at=now() WHERE id=$1',[f.current]);
    expect(await schedule(f.current)).toMatchObject({generation:before.generation,scan_memory_id:before.scan_memory_id});
    await pool.query("UPDATE memories SET scope_key='s2' WHERE id=$1",[f.current]);
    const after=await schedule(f.current);
    expect(after.generation).not.toBe(before.generation);expect(after.revision).not.toBe(before.revision);
    expect(after).toMatchObject({scan_memory_id:null,scan_reference_date:null,scan_similarity:null});
    await drain({arbitrateConflict:vi.fn()} as never);
    expect(await schedule(f.current)).toMatchObject({available_at:new Date('2099-01-01T00:00:00Z'),scan_memory_id:null});
  });
  it('restarts yesterday\'s cursor from the beginning using the database UTC date, not JS clock or session timezone',async()=>{
    const f=await fixture(false,1,false),first=f.keys[0];
    // Become eligible today without another write after the cursor is installed.
    await pool.query("UPDATE memories SET valid_from=(statement_timestamp() AT TIME ZONE 'UTC')::date WHERE vault_id=$1",[f.vaultId]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[f.vaultId,f.current]);
    await pool.query(`UPDATE memory_contradiction_schedule SET scan_reference_date=(statement_timestamp() AT TIME ZONE 'UTC')::date-1,
      scan_similarity=1,scan_memory_id=$2 WHERE memory_id=$1`,[f.current,first]);
    vi.useFakeTimers({toFake:['Date']});vi.setSystemTime(new Date('2000-01-01T00:00:00Z'));
    try{
      const observed=observeReads(),extractor=fixtureExtractor(async()=>'keep_both' as const);
      await drain(extractor as never);
      expect(observed.reads.find(r=>!r.current)?.key).toBeNull();
      expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();expect(await schedule(f.current)).toBeUndefined();
    }finally{vi.useRealTimers();}
  });
  it('cannot save a consumed cursor over a concurrently replaced generation',async()=>{
    const f=await fixture(),original=Client.prototype.query;let raced=false;
    vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      if(!raced && typeof args[0]==='string' && args[0].includes('SET available_at = now()')){
        raced=true;
        return pool.query("UPDATE memories SET data='A corrected workflow' WHERE id=$1",[f.current])
          .then(()=>Reflect.apply(original,this,args));
      }
      return Reflect.apply(original,this,args);
    } as any);
    const before=await schedule(f.current);await drain({arbitrateConflict:vi.fn()} as never);
    const after=await schedule(f.current);
    expect(raced).toBe(true);expect(after.generation).not.toBe(before.generation);
    expect(after).toMatchObject({scan_memory_id:null,scan_reference_date:null,scan_similarity:null,failures:0});
    expect(await logs(f.vaultId)).toEqual([]);
  });
  it('retains the unprocessed actionable lookahead for the next persisted pass',async()=>{
    const f=await fixture(false,2,false);
    await pool.query("UPDATE memories SET type='system_fact' WHERE vault_id=$1",[f.vaultId]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[f.vaultId,f.current]);
    const extractor=fixtureExtractor(async()=>'keep_both' as const);
    await drain(extractor as never);
    expect((await schedule(f.current)).scan_memory_id).toBe(f.keys[0]);
    expect(await logs(f.vaultId)).toHaveLength(1);
    await due(f.vaultId);await drain(extractor as never);
    expect(await schedule(f.current)).toBeUndefined();expect(await logs(f.vaultId)).toHaveLength(2);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(2);
  });
  it('retries from durable progress after connection loss between consumption and cursor save',async()=>{
    const f=await fixture(true),extractor={arbitrateConflict:vi.fn()};
    await drain(extractor as never);
    const saved=await schedule(f.current),original=Client.prototype.query;
    let interrupted=false;
    const hook=vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]){
      if(!interrupted && typeof args[0]==='string' && args[0].includes('SET available_at = now()')){
        interrupted=true;
        return pool.query(`SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity
          WHERE datname=current_database() AND application_name='persistio:contradiction-activation'`)
          .then(rows=>{
            expect(rows.rows).toEqual([{stopped:true}]);
            return Reflect.apply(original,this,args);
          });
      }
      return Reflect.apply(original,this,args);
    } as any);
    await due(f.vaultId);await expect(drain(extractor as never)).rejects.toThrow();
    hook.mockRestore();expect(interrupted).toBe(true);
    expect(await schedule(f.current)).toMatchObject({generation:saved.generation,revision:saved.revision,
      scan_memory_id:saved.scan_memory_id,scan_reference_date:saved.scan_reference_date,scan_similarity:saved.scan_similarity});
    expect(await logs(f.vaultId)).toEqual([]);
    const observed=observeReads();
    await drain(extractor as never);
    expect(observed.reads.find(r=>!r.current)?.key).toBe(saved.scan_memory_id);
    expect(observed.reads.reduce((n,r)=>n+r.count,0)).toBeLessThanOrEqual(scanner.CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    expect((await schedule(f.current)).scan_memory_id).not.toBe(saved.scan_memory_id);
    await due(f.vaultId);await drain(extractor as never);
    expect(await schedule(f.current)).toMatchObject({available_at:new Date('2099-01-01T00:00:00Z'),scan_memory_id:null});
    expect(await logs(f.vaultId)).toEqual([]);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
  });
  it('rejects partial/invalid cursor state and guards down while progress is retained',async()=>{
    const f=await fixture();
    await expect(pool.query('UPDATE memory_contradiction_schedule SET scan_memory_id=$2 WHERE memory_id=$1',[f.current,f.keys[0]]))
      .rejects.toHaveProperty('code','23514');
    await expect(pool.query(`UPDATE memory_contradiction_schedule SET scan_memory_id=$2,scan_reference_date=DATE '2026-09-18',
      scan_similarity='NaN'::float8 WHERE memory_id=$1`,[f.current,f.keys[0]])).rejects.toHaveProperty('code','23514');
    await drain({arbitrateConflict:vi.fn()} as never);
    const before=await schedule(f.current),down=readFileSync(resolve(__dirname,'../../db/migrations/down/059_bounded_contradiction_progress.sql'),'utf8');
    const client=await pool.connect();
    try{
      await client.query('BEGIN');await expect(client.query(down)).rejects.toThrow('retained cursors');await client.query('ROLLBACK');
      expect(await schedule(f.current)).toEqual(before);
      // Transactional test only: explicit cursor reset followed by down/up, all rolled back.
      await client.query('BEGIN');
      await client.query('UPDATE memory_contradiction_schedule SET scan_memory_id=NULL,scan_reference_date=NULL,scan_similarity=NULL WHERE memory_id=$1',[f.current]);
      await client.query(down);
      await client.query(readFileSync(resolve(__dirname,'../../db/migrations/059_bounded_contradiction_progress.sql'),'utf8'));
      const row=(await client.query('SELECT scan_memory_id FROM memory_contradiction_schedule WHERE memory_id=$1',[f.current])).rows[0];
      expect(row).toEqual({scan_memory_id:null});await client.query('ROLLBACK');
      expect(await schedule(f.current)).toEqual(before);
    }finally{await client.query('ROLLBACK');client.release();}
  });
});
