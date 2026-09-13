import { beforeEach, describe, expect, it, vi } from 'vitest';
const mocks=vi.hoisted(()=>({reserve:vi.fn(),publish:vi.fn(),extractor:vi.fn()}));
vi.mock('../crypto',()=>({isVaultEncryptionActive:()=>false}));
vi.mock('../entity-resolver',()=>({normaliseSubject:(s:string)=>s.toLowerCase().trim(),resolveCanonical:async()=>null}));
vi.mock('../usage',()=>({reserveMemoryCreationInTransaction:mocks.reserve}));
vi.mock('../worker-effects',()=>({publishCommittedWorkerEffects:mocks.publish}));
vi.mock('../extractor',()=>({ExtractorService:class{arbitrateConflict=mocks.extractor;}}));
import {deduplicateMemoryInTransaction,fingerprintDedupInput,getDedupEscalationRequest,type DedupInput} from '../dedup';
const prepared={assertCurrent:vi.fn(async()=>{}),encrypt:(_v:unknown,s:string)=>s,decrypt:(_v:unknown,s:string)=>s,
  subject:()=>null,subjectMatch:(_v:unknown,s:string)=>s};
const input=():DedupInput=>({vaultId:'vault',fact:'Incoming durable fact',subject:'topic',embedding:[1,0],sourceChunks:['source'],
  score:8,salience:0.8,sensitivity:'low',type:'system_fact',scope:'session',scopeKey:'s1',polarity:'neutral',
  status:'active',volatility:'low',validFrom:null,validUntil:null});
function database(options:{none?:boolean;exact?:boolean;revision?:string;missingSource?:boolean;similarity?:number}={}){
  const row={id:'existing',data:options.exact?input().fact:'Prior durable fact',subject:'topic',scope:'session',scope_key:'s1',
    polarity:'neutral',type:'system_fact',status:'active',row_version:'1',valid_from:null,valid_until:null,
    source_chunks:['prior-source'],sensitivity:'low',similarity:options.similarity??0.9};
  return{query:vi.fn(async(sql:string,_values?:unknown[])=>{
    if(sql.includes('FROM vaults'))return{rowCount:1,rows:[{id:'vault',encrypted_dek:null,vault_encryption_enabled:false}]};
    if(sql.includes('FROM raw_chunks'))return{rowCount:options.missingSource?0:1,rows:[{id:'source'}]};
    if(sql.includes('FROM memories m WHERE'))return{rowCount:options.none?0:1,rows:options.none?[]:[row]};
    if(sql.includes('FOR UPDATE'))return{rowCount:1,rows:[{row_version:options.revision??'1'}]};
    if(sql.includes('INSERT INTO memories'))return{rowCount:1,rows:[{id:'inserted'}]};
    return{rowCount:1,rows:[]};
  })};
}
describe('prepared baseline dedup decisions',()=>{
  beforeEach(()=>{vi.clearAllMocks();mocks.reserve.mockResolvedValue({});});
  const decision=(value:DedupInput)=>({precomputedConflictInput:fingerprintDedupInput(value),precomputedConflictMemoryId:'existing',
    precomputedConflictMemoryRevision:'1',precomputedConflictDecision:'merge' as const});
  it('uses a matching prepared decision without live model I/O or rewriting supported text',async()=>{
    const value=input(),db=database();
    expect(await deduplicateMemoryInTransaction(value,db as never,prepared,[],decision(value))).toEqual({action:'updated',memoryId:'existing'});
    expect(db.query.mock.calls.some(([sql])=>sql.includes('SET source_chunks'))).toBe(true);
    expect(db.query.mock.calls.some(([sql])=>sql.includes('SET data'))).toBe(false);
    expect(mocks.extractor).not.toHaveBeenCalled();
  });
  it.each(['target','revision','input'])('retains the new fact separately for stale %s decisions',async change=>{
    const value=input(),db=database(),options=decision(value);
    if(change==='target')options.precomputedConflictMemoryId='other';
    if(change==='revision')options.precomputedConflictMemoryRevision='0';
    if(change==='input')options.precomputedConflictInput='stale';
    expect((await deduplicateMemoryInTransaction(value,db as never,prepared,[],options)).action).toBe('inserted');
    expect(db.query.mock.calls.some(([sql])=>sql.startsWith('UPDATE memories'))).toBe(false);
    expect(mocks.extractor).not.toHaveBeenCalled();
  });
  it('requires unchanged locked revisions even after matching input selection',async()=>{
    const value=input(),db=database({revision:'2'});
    await expect(deduplicateMemoryInTransaction(value,db as never,prepared,[],decision(value))).rejects.toThrow('changed during');
    expect(mocks.reserve).not.toHaveBeenCalled();
    expect(db.query.mock.calls.some(([sql])=>sql.startsWith('UPDATE memories'))).toBe(false);
  });
  it('bounds same-subject same-binding similarity without excluding history by current time',async()=>{
    const db=database();await getDedupEscalationRequest(input(),'request',db as never,prepared);
    const sql=db.query.mock.calls.find(([sql])=>sql.includes('FROM memories m WHERE'))![0];
    expect(sql).toContain('m.scope_key IS NOT DISTINCT FROM');
    expect(sql).toContain('m.subject=$5');expect(sql).toContain('LIMIT 10');
    expect(sql).toContain("m.status='active'");expect(sql).not.toContain('current_date');
    expect(sql).not.toContain('authority');expect(sql).not.toContain('now()');
  });
  it('treats even very high similarity as a signal, never automatic equivalence',async()=>{
    const db=database({similarity:0.999});
    expect((await deduplicateMemoryInTransaction(input(),db as never,prepared,[])).action).toBe('inserted');
    expect(mocks.extractor).not.toHaveBeenCalled();
  });
  it('collects effects for the caller to publish only after commit',async()=>{
    const effects:any[]=[];
    await deduplicateMemoryInTransaction(input(),database({none:true}) as never,prepared,effects);
    expect(effects.map(e=>e.kind)).toEqual(['quota','memory-count']);
    expect(mocks.publish).not.toHaveBeenCalled();
  });
  it.each([
    {status:'needs_review'}, {status:'candidate'}, {policyRejections:[{code:'invalid',field:'scope',reason:'invalid'}]},
    {fact:'api_key=sk-example-secret-value-123456789'}, {scope:'unknown'}, {scopeKey:null},
    {validFrom:'2026-02-30'}, {validFrom:'2026-12-01',validUntil:'2026-01-01'}, {salience:NaN}
  ])('rejects invalid input before reads or writes: %j',extra=>{
    const db=database();
    return expect(deduplicateMemoryInTransaction({...input(),...extra} as any,db as never,prepared,[])).rejects.toThrow()
      .then(()=>expect(db.query).not.toHaveBeenCalled());
  });
  it('checks source ownership and encryption identity before any memory write',async()=>{
    const db=database({missingSource:true});
    await expect(deduplicateMemoryInTransaction(input(),db as never,prepared,[])).rejects.toThrow('does not belong');
    expect(prepared.assertCurrent).toHaveBeenCalledOnce();
    expect(db.query.mock.calls.some(([sql])=>sql.includes('INSERT INTO memories'))).toBe(false);
  });
});
