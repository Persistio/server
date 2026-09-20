import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import type { DedupInput } from '../dedup';
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('arbitration authorization (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:url}),vaults:string[]=[];
  let db:typeof import('../../db/client'),dedup:typeof import('../dedup'),vector:number[];
  beforeAll(async()=>{
    process.env.DATABASE_URL=url;
    db=await import('../../db/client');await db.runMigrations();
    const config=(await import('../../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    vector=[1,...Array(config.STORAGE_EMBEDDING_DIMENSIONS-1).fill(0)];dedup=await import('../dedup');
  });
  afterEach(async()=>{await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;});
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function fixture(){
    const vaultId=crypto.randomUUID();vaults.push(vaultId);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'authorization-test',$2,'unlimited')",[vaultId,crypto.randomUUID()]);
    const sources=[crypto.randomUUID(),crypto.randomUUID()];
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role) SELECT unnest($1::uuid[]),$2,'test','user'",[sources,vaultId]);
    const input:DedupInput={vaultId,fact:'The service uses PostgreSQL.',subject:'storage',embedding:vector,sourceChunks:[sources[0]],score:8,salience:0.8,sensitivity:'low',type:'system_fact',scope:'global',scopeKey:null,polarity:'neutral',status:'active',volatility:'low',validFrom:null,validUntil:null};
    const first=await dedup.deduplicateMemory(input);
    return{input:{...input,sourceChunks:[sources[1]]},first};
  }
  it('does not send or retire when retained evidence is unreadable',async()=>{
    const {input}=await fixture();input.fact='The service now uses SQLite.';
    const provider={arbitrateConflict:vi.fn(async()=>'supersede_old')};
    await dedup.deduplicateMemory(input,provider as never);
    expect(provider.arbitrateConflict).not.toHaveBeenCalled();
    expect((await pool.query("SELECT status FROM memories WHERE vault_id=$1",[input.vaultId])).rows).toEqual([{status:'active'},{status:'active'}]);
  });
  it('does not accept a caller-forged destructive enum with matching public identifiers',async()=>{
    const {input,first}=await fixture();input.fact='The service now uses SQLite.';
    const revision=(await pool.query('SELECT revision::text FROM memories WHERE id=$1',[first.memoryId])).rows[0].revision;
    await dedup.deduplicateMemory(input,undefined,{precomputedConflictDecision:'supersede_old',precomputedConflictMemoryId:first.memoryId,precomputedConflictMemoryRevision:revision,precomputedConflictInput:dedup.fingerprintDedupInput(input)});
    expect((await pool.query("SELECT count(*)::int AS n FROM memories WHERE vault_id=$1 AND status='active'",[input.vaultId])).rows[0].n).toBe(2);
  });
  it('does not consolidate identical wording with different source manifests',async()=>{
    const {input}=await fixture();
    expect((await dedup.deduplicateMemory(input)).action).toBe('inserted');
    expect((await pool.query("SELECT count(*)::int AS n FROM memories WHERE vault_id=$1 AND status='active'",[input.vaultId])).rows[0].n).toBe(2);
  });
  it('does not treat two empty manifests as proof of an exact source retry',async()=>{
    const {input}=await fixture();input.fact='An unbound observation.';input.sourceChunks=[];
    await dedup.deduplicateMemory(input);expect((await dedup.deduplicateMemory(input)).action).toBe('inserted');
  });

});
