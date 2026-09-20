import {beforeEach,describe,it,expect,vi} from 'vitest';
const mocks=vi.hoisted(()=>({query:vi.fn(),write:vi.fn(),tx:vi.fn(),decrypt:vi.fn(),identity:vi.fn(),enqueue:vi.fn(),publish:vi.fn(),schedulerQuery:vi.fn(),remind:vi.fn()}));
vi.mock('pg',()=>({Client:class {query=mocks.schedulerQuery;connect=vi.fn();end=vi.fn();on=vi.fn();}}));
vi.mock('../config',()=>({getConfig:()=>({CONTRADICTION_SCAN_ENABLED:true,CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH:4,CONTRADICTION_SCAN_MIN_SIMILARITY:0.8})}));
vi.mock('../db/client',()=>({query:(sql:string,...args:unknown[])=>sql.startsWith('WITH future_memories')?mocks.remind(sql,...args):mocks.query(sql,...args),withTransaction:mocks.tx}));
vi.mock('./crypto',()=>({decryptForVault:mocks.decrypt,prepareVaultCrypto:async()=>({assertCurrent:mocks.identity})}));
vi.mock('./curation-work',()=>({enqueueCurationWork:mocks.enqueue}));
vi.mock('./worker-effects',()=>({publishCommittedWorkerEffects:mocks.publish}));
// Source authorization is exercised with the real database and SDK in arbitration-evidence.integration.test.ts.
vi.mock('./conflict-evidence',async original=>({...await original() as object,prepareConflictEvidence:async()=>({pairId:'test'}),conflictStillCurrent:async()=>true}));
vi.mock('./extractor',()=>({validatedConflictAssessment:(decision:unknown)=>['merge','discard_new','supersede_old','keep_both'].includes(decision as string)?{decision,evidence:{}}:undefined}));
import {scanForContradictions,CONTRADICTION_MAX_CANDIDATES_PER_PASS} from './contradiction-scanner';
import {drainDueContradictionActivations} from './contradiction-activation';
const current={id:'vault',memory_id:'current',data:'Current supported fact',status:'active',scope:'session',scope_key:'s1',
  row_version:'1',type:'system_fact',polarity:'neutral',source_timestamp:'2025-01-01T00:00:00Z',
  valid_from:null as string|null,valid_until:null as string|null,created_at:'2026-01-01T00:00:00Z',similarity:1,account_id:null};
const other={...current,memory_id:'other',data:'Another supported fact',similarity:0.9};
describe('bounded revision-bound contradiction scanning',()=>{
  beforeEach(()=>{
    vi.resetAllMocks();
    mocks.decrypt.mockImplementation(async(_v,s)=>s);
    mocks.write.mockImplementation(async(sql)=>({rowCount:sql.includes('FOR UPDATE')?2:1,rows:[]}));
    mocks.tx.mockImplementation(async fn=>fn({query:mocks.write}));
    mocks.query.mockResolvedValue({rowCount:1,rows:[{unchanged:true}]});
  });
  const select=(a=current,neighbors=[other])=>mocks.query.mockResolvedValueOnce({rowCount:1,rows:[a]}).mockResolvedValueOnce({rowCount:neighbors.length,rows:neighbors});
  it.each(['user_rule','user_preference','task_pattern','workflow','constraint','decision'])(
    'retains a future %s reminder even when SQL excludes every nonidentical neighbor',async type=>{
      select({...current,type,valid_from:'2099-01-01'},[]);
      const budget={remaining:4},extractor={arbitrateConflict:vi.fn()};
      const result=await scanForContradictions('vault',['current'],extractor as never,{budget,now:new Date('2026-09-18')});
      expect(result).toEqual({completedMemoryIds:[],deferredMemoryIds:['current'],reactivations:{current:{
        revision:'1',referenceDate:'2026-09-18',validFrom:'2099-01-01'
      }}});
      expect(mocks.remind).toHaveBeenCalledOnce();expect(budget.remaining).toBe(4);
      expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(mocks.tx).not.toHaveBeenCalled();
    });
  it('preserves all discovered future dates before pair logs without replacing earlier due work',async()=>{
    const future={...other,type:'workflow',valid_from:'2099-02-01'};
    select(current,[future,{...future,memory_id:'earlier',valid_from:'2099-01-01',similarity:0.8}]);
    const result=await scanForContradictions('vault',['current'],{} as never,{maxArbitrations:1,now:new Date('2026-09-18')});
    expect(result.cursors?.current.memoryId).toBe('other');
    expect(result.reactivations).toBeUndefined();
    const [sql,params]=mocks.remind.mock.calls[0];
    expect(JSON.parse(params[1])).toEqual([{id:'other',revision:'1'},{id:'earlier',revision:'1'}]);
    expect(sql).toContain('m.revision=observed.revision');expect(sql).toContain('ORDER BY m.id FOR NO KEY UPDATE OF m');
    expect(sql).toContain("valid_from::timestamp AT TIME ZONE 'UTC'");expect(sql).toContain('ON CONFLICT(memory_id) DO NOTHING');
    expect(mocks.remind.mock.invocationCallOrder[0]).toBeLessThan(mocks.tx.mock.invocationCallOrder[0]);
  });
  it.each([{type:'workflow',valid_until:'2020-01-01'},{type:'workflow',valid_from:'infinity'},
    {type:null,valid_from:'2099-01-01'},{type:'system_fact',valid_from:'2099-01-01'}])(
    'does not retain a future behavioral reminder for %j',async fields=>{
      select({...current,...fields} as any,[]);
      const result=await scanForContradictions('vault',['current'],{} as never,{now:new Date('2026-09-18')});
      expect(result).toEqual({completedMemoryIds:['current'],deferredMemoryIds:[]});
      expect(mocks.remind).not.toHaveBeenCalled();
    });
  it('does not save progress or pair decisions if preserving a future neighbor fails',async()=>{
    select(current,[{...other,type:'workflow',valid_from:'2099-01-01'}]);
    mocks.remind.mockRejectedValueOnce(Error('Connection lost before reminder insert'));
    await expect(scanForContradictions('vault',['current'],{} as never)).rejects.toThrow('Connection lost');
    expect(mocks.tx).not.toHaveBeenCalled();
  });
  it.each(['merge','discard_new'])('stops when %s retires the current memory',async decision=>{
    select(current,[other,{...other,memory_id:'later'}]);
    const extractor={arbitrateConflict:vi.fn(async()=>decision)};
    const result=await scanForContradictions('vault',['current'],extractor as never);
    expect(result.completedMemoryIds).toEqual(['current']);expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });
  it.each(['user_rule','user_preference','task_pattern','workflow','constraint','decision'])(
    'excludes non-current %s from model arbitration without recording a model decision',async type=>{
      for(const bounds of [{valid_from:null,valid_until:'2020-01-01'},{valid_from:'2099-01-01',valid_until:null}]){
        select({...current,type,...bounds} as any,[{...other,type,...bounds} as any]);
        const extractor={arbitrateConflict:vi.fn()},budget={remaining:4};
        await scanForContradictions('vault',['current'],extractor as never,{budget,now:new Date('2026-09-18')});
        expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(budget.remaining).toBe(4);
        expect(mocks.tx).not.toHaveBeenCalled();
      }
    });
  it.each(['user_rule','user_preference','task_pattern','workflow','constraint','decision'])(
    'admits %s on both inclusive UTC validity bounds',async type=>{
      const bounds={valid_from:'2026-09-18',valid_until:'2026-09-18'};
      select({...current,type,...bounds} as any,[{...other,type,...bounds} as any]);
      const extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
      await scanForContradictions('vault',['current'],extractor as never,{now:new Date('2026-09-18T00:30:00Z')});
      expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    });
  it.each(['project','system_fact','domain_knowledge'])(
    'retains historical/future %s model arbitration',async type=>{
      for(const bounds of [{valid_from:null,valid_until:'2020-01-01'},{valid_from:'2099-01-01',valid_until:null}]){
        select({...current,type,...bounds} as any,[{...other,type,...bounds} as any]);
        const extractor={arbitrateConflict:vi.fn(async()=>'keep_both')};
        await scanForContradictions('vault',['current'],extractor as never,{now:new Date('2026-09-18')});
        expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
      }
    });
  it.each([{type:null},{type:'unknown'},{valid_from:'2026-02-30'},{valid_until:'malformed'}])(
    'fails closed for malformed model context %j',async fields=>{
      select({...current,...fields} as any,[{...other,...fields} as any]);
      const extractor={arbitrateConflict:vi.fn()};
      await scanForContradictions('vault',['current'],extractor as never);
      expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(mocks.tx).not.toHaveBeenCalled();
    });
  it('preserves deterministic exact dedup of non-current behavior without a model call',async()=>{
    select({...current,type:'user_rule',valid_until:'2020-01-01'} as any,
      [{...other,type:'user_rule',valid_until:'2020-01-01',data:current.data} as any]);
    const extractor={arbitrateConflict:vi.fn()};
    await scanForContradictions('vault',['current'],extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(mocks.tx).not.toHaveBeenCalled();
  });
  it('completes the actual scheduler maxArbitrations:1 pass with excluded first and exact second',async()=>{
    const dated={...current,type:'user_rule',valid_until:'2020-01-01'};
    mocks.schedulerQuery.mockImplementation(async(sql:string,args:unknown[]=[])=>{
      if(sql.includes('SELECT vault_id, revision::text'))return{rows:[{vault_id:'vault',revision:'1'}]};
      if(sql.includes('pg_try_advisory_lock'))return{rows:[{locked:true}]};
      if(sql.includes('SELECT memory_id, generation, failures'))return{rows:[{memory_id:'current',generation:'g1',failures:0,revision:'1',reference_now:new Date('2026-09-18T00:00:00Z')}]};
      if(sql.includes('1.0 AS similarity'))return{rows:[dated]};
      if(sql.includes('FROM memories original'))return{rows:args[7]?[]:[
        {...dated,memory_id:'excluded',data:'Not equal',similarity:1},
        {...dated,memory_id:'exact',similarity:0.9}
      ]};
      if(sql.includes('FOR UPDATE'))return{rowCount:2,rows:[]};
      return{rowCount:1,rows:[]};
    });
    const extractor={arbitrateConflict:vi.fn()};
    await drainDueContradictionActivations(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    const calls=mocks.schedulerQuery.mock.calls;
    expect(calls.find(([sql])=>sql.includes('FROM memories original'))![1][4]).toBe(2);
    expect(calls.some(([sql])=>sql.includes('INSERT INTO contradiction_scan_log'))).toBe(false);
    expect(calls.find(([sql])=>sql.startsWith('DELETE FROM memory_contradiction_schedule'))![1]).toEqual(['current','g1','1','2026-09-18']);
    expect(calls.some(([sql])=>sql.includes('SET available_at = now()'))).toBe(false);
    expect(mocks.query).not.toHaveBeenCalled();
  });
  it.each([false,true])('pages past unverified hash hints at cap one (exact later=%s)',async exact=>{
    const dated={...current,type:'workflow',valid_from:'2099-01-01'};
    const skipped={...dated,data:'Not plaintext equality',memory_id:'hint-a',similarity:0.95};
    const last={...skipped,memory_id:'hint-b',similarity:0.9};
    select(dated,[skipped,last]);
    mocks.query.mockResolvedValueOnce({rows:exact?[{...dated,memory_id:'exact',similarity:0.85}]:[]});
    const extractor={arbitrateConflict:vi.fn()},budget={remaining:4};
    const result=await scanForContradictions('vault',['current'],extractor as never,{maxArbitrations:1,budget});
    expect(result).toEqual({completedMemoryIds:[],deferredMemoryIds:['current'],
      reactivations:{current:{revision:'1',referenceDate:expect.any(String),validFrom:'2099-01-01'}}});
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(budget.remaining).toBe(4);
    const pages=mocks.query.mock.calls.filter(([sql])=>sql.includes('FROM memories original'));
    expect(pages).toHaveLength(2);expect(pages[1][1].slice(4)).toEqual([2,false,0.9,'hint-b']);
    expect(mocks.tx).not.toHaveBeenCalled();
  });
  it('exhausts only skipped hints before declaring completion of an unchanged revision',async()=>{
    const dated={...current,type:'workflow',valid_from:'2099-01-01'};
    select(dated,[{...dated,memory_id:'hint',data:'Not equal'}]);
    mocks.query.mockResolvedValueOnce({rowCount:0,rows:[]});
    const result=await scanForContradictions('vault',['current'],{arbitrateConflict:vi.fn()} as never,{maxArbitrations:1});
    expect(result.deferredMemoryIds).toEqual(['current']);expect(mocks.tx).not.toHaveBeenCalled();
  });
  it('keeps deterministic different-meaning work capped while skipping unverified hints',async()=>{
    const dated={...current,type:'workflow',valid_from:'2099-01-01'};
    const skipped={...dated,memory_id:'hint',data:'Not equal',similarity:0.99};
    const distinct={...dated,memory_id:'distinct-a',polarity:'negative',similarity:0.95};
    select(dated,[skipped,distinct]);
    mocks.query.mockResolvedValueOnce({rows:[{...distinct,memory_id:'distinct-b',similarity:0.9}]});
    const extractor={arbitrateConflict:vi.fn()},budget={remaining:4};
    const result=await scanForContradictions('vault',['current'],extractor as never,{maxArbitrations:1,budget});
    expect(result.deferredMemoryIds).toEqual(['current']);expect(mocks.tx).toHaveBeenCalledOnce();
    expect(mocks.write.mock.calls.find(([sql])=>sql.includes('INSERT INTO contradiction_scan_log'))![1].slice(1,4)).toEqual(['distinct-a','current','incomplete']);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(budget.remaining).toBe(4);
  });
  it('does not report completion when a later page is empty because the current revision changed',async()=>{
    const dated={...current,type:'workflow',valid_from:'2099-01-01'};
    select(dated,[{...dated,memory_id:'hint',data:'Not equal'},
      {...dated,memory_id:'distinct',polarity:'negative'}]);
    mocks.query.mockResolvedValueOnce({rows:[]}).mockResolvedValueOnce({rowCount:0,rows:[]});
    const result=await scanForContradictions('vault',['current'],{arbitrateConflict:vi.fn()} as never,{maxArbitrations:1});
    expect(result.deferredMemoryIds).toEqual(['current']);expect(mocks.tx).toHaveBeenCalledOnce();
  });
  it('bounds total reads/decryptions across multiple IDs and resumes beyond false hash hints',async()=>{
    const dated={...current,type:'workflow',valid_from:'2099-01-01'};
    const hints=Array.from({length:CONTRADICTION_MAX_CANDIDATES_PER_PASS+4},(_,i)=>({
      ...dated,memory_id:`hint-${i}`,data:`False hint ${i}`,similarity:1-i/1000
    }));
    const pages:unknown[][]=[];
    mocks.query.mockImplementation(async(sql:string,args:unknown[])=>{
      if(sql.includes('1.0 AS similarity'))return{rows:[dated],rowCount:1};
      if(sql.includes('FROM memories original')){
        pages.push(args);
        const start=args[7]===null?0:hints.findIndex(h=>h.memory_id===args[7])+1;
        return{rows:hints.slice(start,start+Number(args[4]))};
      }
      return{rows:[],rowCount:1};
    });
    const extractor={arbitrateConflict:vi.fn()};
    const first=await scanForContradictions('vault',['current','second'],extractor as never,{maxArbitrations:1});
    expect(first.deferredMemoryIds).toEqual(['current','second']);
    expect(mocks.decrypt).toHaveBeenCalledTimes(CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    expect(mocks.remind.mock.calls.reduce((count,[,params])=>count+JSON.parse(params[1]).length,0))
      .toBe(CONTRADICTION_MAX_CANDIDATES_PER_PASS);
    expect(first.cursors?.current.memoryId).toBe(hints[CONTRADICTION_MAX_CANDIDATES_PER_PASS-2].memory_id);
    expect(mocks.tx).not.toHaveBeenCalled();
    const count=pages.length;
    const second=await scanForContradictions('vault',['current'],extractor as never,{maxArbitrations:1,cursors:first.cursors});
    expect(second.deferredMemoryIds).toEqual(['current']);
    expect(second.reactivations?.current.validFrom).toBe('2099-01-01');
    expect(pages[count][7]).toBe(first.cursors?.current.memoryId);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
  });
  it('does not advance past unprocessed actionable lookahead',async()=>{
    select(current,[other,{...other,memory_id:'lookahead',similarity:0.8}]);
    const first=await scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'keep_both'} as never,{maxArbitrations:1});
    expect(first.cursors?.current.memoryId).toBe('other');
  });
  it.each(['revision','date'])('discards stale %s cursor before reading candidates',async changed=>{
    select(current,[]);
    await scanForContradictions('vault',['current'],{} as never,{now:new Date('2026-09-18'),cursors:{current:{
      revision:changed==='revision'?'2':'1',referenceDate:changed==='date'?'2026-09-17':'2026-09-18',
      similarity:0.9,memoryId:'stale'
    }}});
    expect(mocks.query.mock.calls[1][1].slice(6)).toEqual([null,null]);
  });
  it('returns no progress after a concurrent revision change',async()=>{
    select(current,[other,{...other,memory_id:'lookahead'}]);
    mocks.query.mockResolvedValueOnce({rowCount:0,rows:[]});
    const result=await scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'keep_both'} as never,{maxArbitrations:1});
    expect(result).toEqual({completedMemoryIds:[],deferredMemoryIds:['current']});
  });
  it('verifies decrypted plaintext rather than treating a stored hash as equality',async()=>{
    const dated={...current,type:'workflow',valid_from:'2099-01-01',data:'cipher-current'};
    select(dated,[{...dated,memory_id:'false-hint',data:'cipher-other'},
      {...dated,memory_id:'exact',data:'cipher-exact'}]);
    mocks.query.mockResolvedValueOnce({rows:[]});
    mocks.decrypt.mockImplementation(async(_v,s)=>s==='cipher-other'?'Different plaintext':'Same plaintext');
    const budget={remaining:1},extractor={arbitrateConflict:vi.fn()};
    const result=await scanForContradictions('vault',['current'],extractor as never,{maxArbitrations:1,budget});
    expect(result.deferredMemoryIds).toEqual(['current']);expect(budget.remaining).toBe(1);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();expect(mocks.tx).not.toHaveBeenCalled();
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
      evidence:{pairId:'test'},existing:{sourceTimestamp:other.source_timestamp,validFrom:null,validUntil:null,createdAt:other.created_at},
      incoming:{sourceTimestamp:current.source_timestamp,validFrom:null,validUntil:null,createdAt:current.created_at}});
  });
  it('records incomplete for an unregistered decision without retiring memory',async()=>{
    select();await scanForContradictions('vault',['current'],{arbitrateConflict:async()=>'needs_review'} as never);
    expect(mocks.write.mock.calls.some(([sql])=>sql.startsWith('UPDATE memories'))).toBe(false);
  });
});
