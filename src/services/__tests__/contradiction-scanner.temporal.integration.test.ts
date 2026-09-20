import {fixtureSource,cleanFixtureSources,fixtureExtractor,fixtureAssessment} from '../../__tests__/arbitration-fixtures';
import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

// Only the external KMS unwrap is stubbed. AES-GCM, SQL, revision/activation
// triggers, scheduler ownership, transactions and constraint checks are real.
const kms=vi.hoisted(()=>({unwrap:vi.fn()}));
vi.mock('@google-cloud/kms',()=>({KeyManagementServiceClient:class {decrypt=kms.unwrap;}}));
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
const now=new Date('2026-09-18T00:00:00Z');
const windows=[{from:null,until:'2020-01-01'},{from:'2099-01-01',until:null}];
type Window={from:string|null;until:string|null};

describe.skipIf(!databaseUrl)('temporal contradiction scan progress (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl}),vaults:string[]=[];
  const dek=Buffer.alloc(32,19);
  let db:typeof import('../../db/client'),crypt:typeof import('../crypto');
  let config:ReturnType<typeof import('../../config').getConfig>;
  let scan:typeof import('../contradiction-scanner').scanForContradictions;
  let drain:typeof import('../contradiction-activation').drainDueContradictionActivations;
  let vector:string;
  beforeAll(async()=>{
    process.env.DATABASE_URL=databaseUrl;
    db=await import('../../db/client');await db.runMigrations();
    config=(await import('../../config')).getConfig();
    config.ENCRYPTION_ENABLED=true;config.KEY_PROVIDER='gcp_kms';
    config.GCP_KMS_KEY_NAME='projects/test/locations/global/keyRings/test/cryptoKeys/test';
    crypt=await import('../crypto');await crypt.initCryptoClient();
    ({scanForContradictions:scan}=await import('../contradiction-scanner'));
    ({drainDueContradictionActivations:drain}=await import('../contradiction-activation'));
    const type=(await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector=JSON.stringify([1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)]);
    const guards=await pool.query(`SELECT tgname,tgenabled FROM pg_trigger WHERE tgrelid='memories'::regclass
      AND tgname IN ('memories_maintain_revision','memories_schedule_contradiction_activation') ORDER BY tgname`);
    expect(guards.rows).toEqual([
      {tgname:'memories_maintain_revision',tgenabled:'O'},
      {tgname:'memories_schedule_contradiction_activation',tgenabled:'O'}
    ]);
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
  const hash=(text:string)=>crypto.createHash('sha256').update(text).digest('hex');
  async function vault(encrypted:boolean){
    const id=crypto.randomUUID();vaults.push(id);
    await pool.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id,encrypted_dek,vault_encryption_enabled)
      VALUES($1,'temporal-scan-progress',$2,'unlimited',$3,$4)`,[id,crypto.randomUUID(),encrypted?'test-wrapped-dek':null,encrypted]);
    return{id,encrypted};
  }
  async function memory(v:Awaited<ReturnType<typeof vault>>,id:string,text:string,dates:Window,
    extra:{hash?:string;type?:string;polarity?:string;scopeKey?:string;similarity?:number}={}){
    const embedding=extra.similarity===undefined?vector:JSON.stringify([
      extra.similarity,Math.sqrt(1-extra.similarity**2),...JSON.parse(vector).slice(2)
    ]);
    await pool.query(`INSERT INTO memories(id,vault_id,data,subject,hash,embedding,scope,scope_key,status,type,polarity,valid_from,valid_until,confidence,source_chunks)
      VALUES($1,$2,$3,'Topic',$4,$5::vector,'session',$6,'active',$7,$8,$9::date,$10::date,0.9,ARRAY[$11::uuid])`,
    [id,v.id,v.encrypted?crypt.encryptField(text,dek):text,extra.hash??hash(text),embedding,extra.scopeKey??'s1',extra.type??'workflow',extra.polarity??'neutral',dates.from,dates.until,await fixtureSource(pool,v.id,text)]);
    await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)',[id,embedding]);
    return id;
  }
  async function fixture(encrypted:boolean,dates:Window,falseHints:boolean,exact:boolean){
    const v=await vault(encrypted),prefix=crypto.randomUUID().slice(0,24);
    const id=(n:number)=>prefix+String(n).padStart(12,'0');
    const text='Use the dated workflow.';
    const excluded=await memory(v,id(1),'A distinct dated workflow.',dates);
    const hints:string[]=[];
    if(falseHints)for(const n of [2,3])hints.push(await memory(v,id(n),'Unverified hash hint '+n,dates,{hash:hash(text)}));
    // Cover both strict similarity descent and UUID tie-breaking across pages.
    const duplicate=exact?await memory(v,id(4),text,dates,{similarity:dates.until?0.9:1}):null;
    const current=await memory(v,id(5),text,dates);
    // Exercise precisely the current memory's real scheduler pass, not random
    // fixture insertion order. All production triggers/constraints stay enabled.
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[v.id,current]);
    return{v,current,excluded,hints,duplicate,text};
  }
  const state=async(vaultId:string)=>(await pool.query(`SELECT id,data,hash,status,archived_at,revision::text,scope,scope_key,
    valid_from::text,valid_until::text FROM memories WHERE vault_id=$1 ORDER BY id`,[vaultId])).rows;
  const logs=async(vaultId:string)=>(await pool.query('SELECT memory_id_a,memory_id_b,decision,revision_a::text,revision_b::text FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY memory_id_a',[vaultId])).rows;
  const schedule=async(id:string)=>(await pool.query('SELECT generation,failures FROM memory_contradiction_schedule WHERE memory_id=$1',[id])).rows;
  async function due(vaultId:string){
    await pool.query("UPDATE memory_contradiction_schedule SET available_at=now()-interval '1 second' WHERE vault_id=$1 AND available_at < now()+interval '1 day'",[vaultId]);
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 second' WHERE vault_id=$1",[vaultId]);
  }

  it.each([false,true].flatMap(encrypted=>windows.flatMap(dates=>[false,true].map(falseHints=>({encrypted,dates,falseHints})))))
  ('finishes scheduler cap one, excluded first/exact last: %j',async({encrypted,dates,falseHints})=>{
    const f=await fixture(encrypted,dates,falseHints,true),before=await state(f.v.id);
    const extractor={arbitrateConflict:vi.fn()};
    if(encrypted){
      expect(before.find(m=>m.id===f.current).data).not.toBe(before.find(m=>m.id===f.duplicate).data);
      expect(before.find(m=>m.id===f.current).hash).toBe(before.find(m=>m.id===f.duplicate).hash);
    }
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await schedule(f.current)).toHaveLength(dates.from?1:0);
    const after=await state(f.v.id);
    expect(after).toEqual(before);
    expect(await logs(f.v.id)).toEqual([]);
    expect((await pool.query('SELECT memory_id,revision::text FROM curation_queue_items WHERE vault_id=$1',[f.v.id])).rows)
      .toEqual([]);
    // A second tick cannot invent a decision for skipped future/historical pairs.
    await due(f.v.id);await drain(extractor as never);
    expect(await state(f.v.id)).toEqual(after);expect(await logs(f.v.id)).toHaveLength(0);
  });

  it.each([false,true])('exhausts false hash hints without writes, provider cost or scheduler stall (encrypted=%s)',async encrypted=>{
    const f=await fixture(encrypted,windows[1],true,false),before=await state(f.v.id);
    const extractor=fixtureExtractor(),budget={remaining:1};
    expect(await scan(f.v.id,[f.current],extractor as never,{maxArbitrations:1,budget,now}))
      .toEqual({completedMemoryIds:[],deferredMemoryIds:[f.current],reactivations:{[f.current]:{
        revision:'1',referenceDate:'2026-09-18',validFrom:'2099-01-01'
      }}});
    expect(budget.remaining).toBe(1);expect(await logs(f.v.id)).toEqual([]);
    await drain(extractor as never);
    expect(await schedule(f.current)).toHaveLength(1);expect(await state(f.v.id)).toEqual(before);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await logs(f.v.id)).toEqual([]);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.v.id])).rows).toEqual([]);
    // Skipped pairs have no durable keep_both log: still arbitrate when applicable.
    expect(await scan(f.v.id,[f.current],extractor as never,{maxArbitrations:1,budget:{remaining:1},now:new Date('2100-01-01')}))
      .toEqual({completedMemoryIds:[],deferredMemoryIds:[f.current],cursors:{[f.current]:{
        revision:'1',referenceDate:'2100-01-01',similarity:1,memoryId:f.excluded
      }}});
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect((await logs(f.v.id)).map(row=>row.decision)).toEqual(['keep_both']);
  });

  it('never authorizes retirement from plaintext/hash equality for inapplicable memories',async()=>{
    const f=await fixture(false,windows[0],false,true),extractor={arbitrateConflict:vi.fn()};
    await pool.query('UPDATE memories SET hash=$2 WHERE id=$1',[f.duplicate,crypto.randomUUID()]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[f.v.id,f.current]);
    await drain(extractor as never);
    expect(await schedule(f.current)).toEqual([]);expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await logs(f.v.id)).toEqual([]);
  });

  it('preserves different-meaning deterministic keep_both progress across cap-one scheduler ticks',async()=>{
    const f=await fixture(true,windows[1],true,false),prefix=crypto.randomUUID().slice(0,24);
    const ids=[];
    ids.push(await memory(f.v,prefix+'000000000001','A differently dated statement.',windows[0]));
    ids.push(await memory(f.v,prefix+'000000000002','A distinct decision.',windows[1],{type:'decision'}));
    ids.push(await memory(f.v,prefix+'000000000003','A distinct negative statement.',windows[1],{polarity:'negative'}));
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1 AND memory_id<>$2',[f.v.id,f.current]);
    const before=await state(f.v.id),extractor={arbitrateConflict:vi.fn()};
    for(let n=1;n<=3;n++){
      await due(f.v.id);await drain(extractor as never);
      expect(await logs(f.v.id)).toHaveLength(n);
      expect(await schedule(f.current)).toHaveLength(1);
    }
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(await state(f.v.id)).toEqual(before);
    expect((await logs(f.v.id)).map(row=>row.decision)).toEqual(['incomplete','incomplete','incomplete']);
    expect((await logs(f.v.id)).map(row=>row.memory_id_a).sort()).toEqual(ids.sort());
  });

  it.each([
    {type:'workflow',dates:{from:'2026-09-18',until:'2026-09-18'}},
    {type:'system_fact',dates:windows[0]},
    {type:'domain_knowledge',dates:windows[1]}
  ])('retains current behavior and historical/future factual arbitration: %j',async({type,dates})=>{
    const v=await vault(true),a=await memory(v,crypto.randomUUID(),'Older supported statement',dates,{type});
    const b=await memory(v,crypto.randomUUID(),'Newer supported statement',dates,{type});
    const extractor=fixtureExtractor(async()=>'keep_both' as const),budget={remaining:1};
    expect(await scan(v.id,[b],extractor as never,{maxArbitrations:1,budget,now})).toEqual({completedMemoryIds:[b],deferredMemoryIds:[]});
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();expect(budget.remaining).toBe(0);
    expect(await logs(v.id)).toEqual([{memory_id_a:a,memory_id_b:b,decision:'keep_both',revision_a:'1',revision_b:'1'}]);
  });

  it('keeps same plaintext/hash candidates inside exact vault/scope binding with active constraints',async()=>{
    const f=await fixture(true,windows[1],true,false),foreign=await vault(true);
    await memory(f.v,crypto.randomUUID(),f.text,windows[1],{scopeKey:'other-session'});
    await memory(foreign,crypto.randomUUID(),f.text,windows[1]);
    const before=await state(f.v.id),foreignBefore=await state(foreign.id),extractor={arbitrateConflict:vi.fn()};
    expect(await scan(f.v.id,[f.current],extractor as never,{maxArbitrations:1,now})).toEqual({completedMemoryIds:[],deferredMemoryIds:[f.current],reactivations:{[f.current]:{
        revision:'1',referenceDate:'2026-09-18',validFrom:'2099-01-01'
      }}});
    expect(await state(f.v.id)).toEqual(before);expect(await state(foreign.id)).toEqual(foreignBefore);
    expect(await logs(f.v.id)).toEqual([]);expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    await expect(pool.query('UPDATE memories SET confidence=0 WHERE id=$1',[f.current])).rejects.toThrow('Invalid active memory');
    await expect(pool.query("UPDATE memories SET scope_key='' WHERE id=$1",[f.current])).rejects.toHaveProperty('code','23514');
    expect(await state(f.v.id)).toEqual(before);
  });
});
