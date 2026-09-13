import {beforeEach,describe,it,expect,vi} from 'vitest';
const mocks=vi.hoisted(()=>({query:vi.fn(),write:vi.fn(),tx:vi.fn(),decrypt:vi.fn(),identity:vi.fn(),enqueue:vi.fn(),publish:vi.fn()}));
vi.mock('../config',()=>({getConfig:()=>({CONTRADICTION_SCAN_ENABLED:true,CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH:4,CONTRADICTION_SCAN_MIN_SIMILARITY:0.8})}));
vi.mock('../db/client',()=>({query:mocks.query,withTransaction:mocks.tx}));
vi.mock('./crypto',()=>({decryptForVault:mocks.decrypt,prepareVaultCrypto:async()=>({assertCurrent:mocks.identity})}));
vi.mock('./curation-work',()=>({enqueueCurationWork:mocks.enqueue}));
vi.mock('./worker-effects',()=>({publishCommittedWorkerEffects:mocks.publish}));
import {scanForContradictions} from './contradiction-scanner';
const current={id:'vault',memory_id:'current',data:'Current supported fact',status:'active',scope:'session',scope_key:'s1',
  row_version:'1',type:'system_fact',polarity:'neutral',source_timestamp:'2025-01-01T00:00:00Z',
  valid_from:null,valid_until:null,created_at:'2026-01-01T00:00:00Z',similarity:1,account_id:null};
const other={...current,memory_id:'other',data:'Another supported fact',similarity:0.9};
describe('bounded revision-bound contradiction scanning',()=>{
  beforeEach(()=>{
    vi.clearAllMocks();
    mocks.decrypt.mockImplementation(async(_v,s)=>s);
    mocks.write.mockImplementation(async(sql)=>({rowCount:sql.includes('FOR UPDATE')?2:1,rows:[]}));
    mocks.tx.mockImplementation(async fn=>fn({query:mocks.write}));
    mocks.query.mockResolvedValue({rowCount:1,rows:[{unchanged:true}]});
  });
  const select=(a=current,neighbors=[other])=>mocks.query.mockResolvedValueOnce({rowCount:1,rows:[a]}).mockResolvedValueOnce({rowCount:neighbors.length,rows:neighbors});
  it.each(['merge','discard_new'])('stops when %s retires the current memory',async decision=>{
    select(current,[other,{...other,memory_id:'later'}]);
    const extractor={arbitrateConflict:vi.fn(async()=>decision)};
    const result=await scanForContradictions('vault',['current'],extractor as never);
    expect(result.completedMemoryIds).toEqual(['current']);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });
  it('retains useful alternatives with an exact pair revision log and no state changes',async()=>{
    select();const result=await scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'keep_both'} as never);
    expect(result.completedMemoryIds).toEqual(['current']);
    expect(mocks.write.mock.calls.some(([s])=>s.startsWith('UPDATE memories'))).toBe(false);
    expect(mocks.write.mock.calls.find(([s])=>s.includes('INSERT INTO contradiction_scan_log'))![1].slice(1,4)).toEqual(['other','current','keep_both']);
    expect(mocks.publish).not.toHaveBeenCalled();
  });
  const windows=[[null,null,'2020-01-01',null],[null,'2020-12-31','2021-01-01',null],
    ['2020-01-01','2021-01-01','2020-06-01','2022-01-01']];
  it.each(windows.flatMap(w=>[false,true].flatMap(reverse=>[false,true].map(exact=>({w,reverse,exact})))))
  ('preserves different dates without arbitration $w reverse=$reverse exact=$exact',async({w,reverse,exact})=>{
    const [a,b,c,d]=reverse?[w[2],w[3],w[0],w[1]]:w;
    select({...current,valid_from:a,valid_until:b} as any,[{...other,data:exact?current.data:other.data,valid_from:c,valid_until:d} as any]);
    const extractor={arbitrateConflict:vi.fn()},budget={remaining:4};
    await scanForContradictions('vault',['current'],extractor as never,{budget});
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(budget.remaining).toBe(4);
    expect(mocks.write.mock.calls.some(([s])=>s.startsWith('UPDATE memories'))).toBe(false);
  });
  it.each(['type','polarity'])('does not destructively merge different %s even with identical text',async field=>{
    select(current,[{...other,data:current.data,[field]:field==='type'?'decision':'negative'}]);
    const extractor={arbitrateConflict:vi.fn()};
    await scanForContradictions('vault',['current'],extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(mocks.write.mock.calls.some(([s])=>s.startsWith('UPDATE memories'))).toBe(false);
  });
  it.each(['merge','discard_new','supersede_old','keep_both'])('rejects stale locked inputs before applying %s',async decision=>{
    select();mocks.write.mockImplementation(async()=>({rowCount:1,rows:[]}));
    await expect(scanForContradictions('vault',['current'],{arbitrateConflict:async()=>decision} as never)).rejects.toThrow('inputs changed');
    expect(mocks.write.mock.calls.some(([s])=>s.startsWith('UPDATE')||s.startsWith('INSERT'))).toBe(false);
  });
  it('uses the dedicated lock-owning connection for transaction and rolls back a failed application',async()=>{
    const client={query:vi.fn(async(sql:string)=>{
      if(sql.includes('1.0 AS similarity'))return{rows:[current],rowCount:1};
      if(sql.includes('FROM memories original'))return{rows:[other],rowCount:1};
      if(sql.includes('FOR UPDATE'))return{rows:[],rowCount:1};
      return{rows:[],rowCount:1};
    })};
    await expect(scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'merge'} as never,{client:client as never})).rejects.toThrow('changed');
    const sql=client.query.mock.calls.map(([s])=>s);
    expect(sql).toContain('BEGIN ISOLATION LEVEL READ COMMITTED');expect(sql).toContain('ROLLBACK');expect(sql).not.toContain('COMMIT');
    expect(mocks.tx).not.toHaveBeenCalled();expect(mocks.publish).not.toHaveBeenCalled();
  });
  it('counts a failed provider attempt against the shared budget without modifying memory',async()=>{
    select();const budget={remaining:1};
    await expect(scanForContradictions('vault',['current'],{arbitrateConflict:async()=>{throw Error('Provider unavailable');}} as never,{budget})).rejects.toThrow();
    expect(budget.remaining).toBe(0);expect(mocks.tx).not.toHaveBeenCalled();
  });
  it('retains cap-incomplete work and asks for only one lookahead candidate',async()=>{
    select(current,[other,{...other,memory_id:'later'}]);const budget={remaining:1};
    const result=await scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'keep_both'} as never,{budget});
    expect(result.deferredMemoryIds).toEqual(['current']);expect(mocks.query.mock.calls[1][1][4]).toBe(2);
  });
  it.each([0,1])('does not consume concurrently changed empty results (%i matching revision)',async rowCount=>{
    select(current,[]);mocks.query.mockResolvedValue({rowCount,rows:[]});
    const result=await scanForContradictions('vault',['current'],{} as never);
    expect(rowCount?result.completedMemoryIds:result.deferredMemoryIds).toEqual(['current']);
  });
  it('does not decrypt an absent/ineligible current memory',async()=>{
    mocks.query.mockResolvedValueOnce({rowCount:0,rows:[]});
    expect((await scanForContradictions('vault',['current'],{} as never)).completedMemoryIds).toEqual(['current']);
    expect(mocks.decrypt).not.toHaveBeenCalled();
  });
  it.each(['ciphertext corrupt','KMS unavailable'])('leaves technical %s retryable, not quarantined',async message=>{
    select();mocks.decrypt.mockRejectedValueOnce(Error(message));
    await expect(scanForContradictions('vault',['current'],{} as never)).rejects.toThrow(message);
    expect(mocks.tx).not.toHaveBeenCalled();
  });
  it('supplies source chronology separately from insertion order',async()=>{
    select();const extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
    await scanForContradictions('vault',['current'],extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledWith(other.data,current.data,'vault',{
      existing:{sourceTimestamp:other.source_timestamp,validFrom:null,validUntil:null,createdAt:other.created_at},
      incoming:{sourceTimestamp:current.source_timestamp,validFrom:null,validUntil:null,createdAt:current.created_at}});
  });
  it('rejects an unknown decision before a transaction',async()=>{
    select();await expect(scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'needs_review'} as never)).rejects.toThrow('Invalid conflict');
    expect(mocks.tx).not.toHaveBeenCalled();
  });
});
