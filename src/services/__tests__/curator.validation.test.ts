import { describe, expect, it } from 'vitest';
import { CURATOR_SCHEMA_VERSION, validateCuratorResult, type CuratorMemory } from '../curator';

const memory=(id:string,extra:Partial<CuratorMemory>={}):CuratorMemory=>({
  id,subject:'Persistio',data:'Persistio retains a fact.',type:'system_fact',scope:'project',scope_key:'persistio',
  confidence:0.9,salience:0.8,sensitivity:'low',polarity:'neutral',volatility:'low',parent_id:null,
  valid_from:null,valid_until:null,...extra
});
const replacement={statement:'A clarified durable fact.',subject:'Persistio',type:'system_fact',confidence:0.9,salience:0.8,
  sensitivity:'low',polarity:'neutral',volatility:'low',valid_from:null,valid_until:null,evidence:'Reviewed source'};
const plan=(extra:Record<string,unknown>={})=>({schema_version:CURATOR_SCHEMA_VERSION,
  keep:[{id:'M1',reason:'Already useful'}],update:[],consolidate:[],archive:[],edges:[],scope_changes:[],...extra});

describe('whole Curator result validation',()=>{
  it.each([null,{},[],{...plan(),surprise:true},{...plan(),archive:undefined},
    {...plan(),keep:[{id:'M1',reason:''}]}])('rejects incomplete or malformed output %j',value=>{
    expect(()=>validateCuratorResult(value,[memory('one')],[])).toThrow();
  });
  it('requires a disposition for every selected target, without a promotion state',()=>{
    expect(()=>validateCuratorResult(plan(),[memory('one'),memory('two')],[])).toThrow(/Missing/);
    expect(()=>validateCuratorResult(plan({archive:[{id:'M1',reason:'Duplicate',basis:'duplicate'}]}),[memory('one')],[])).toThrow(/Multiple/);
    expect(validateCuratorResult(plan(),[memory('one')],[])).toMatchObject({keep:[{id:'M1'}]});
  });
  it.each(['unknown-id','C1','M3'])('rejects unseen or legacy alias %s',id=>{
    expect(()=>validateCuratorResult(plan({keep:[{id,reason:'Useful'}]}),[memory('one')],[memory('two')])).toThrow();
  });
  it.each(['archive','consolidate'])('rejects graph endpoints retired by %s',kind=>{
    const value=plan({keep:[{id:'M3',reason:'Useful'}],
      ...(kind==='archive'?{archive:[{id:'M1',reason:'Duplicate',basis:'duplicate'},{id:'M2',reason:'Duplicate',basis:'duplicate'}]}:
        {consolidate:[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Equivalent'}]}),
      edges:[{from:'M1',to:'M3',type:'supports',confidence:0.8,reason:'Related'}]});
    expect(()=>validateCuratorResult(value,[memory('one'),memory('two'),memory('three')],[])).toThrow(/retired/);
  });
  it('rejects conflicting mutations of reviewed context as well as selected targets',()=>{
    const value=plan({update:[{id:'M2',memory:replacement,source_refs:['M2'],reason:'Clarify'}],
      archive:[{id:'M2',reason:'Duplicate',basis:'duplicate'}]});
    expect(()=>validateCuratorResult(value,[memory('one')],[memory('two')])).toThrow(/Multiple/);
  });
  it.each([{scope:'global',scope_key:null},{scope_key:'another-project'},{valid_until:'2020-01-01'}] as Partial<CuratorMemory>[])
  ('refuses consolidation across binding or time %j',extra=>{
    const value=plan({keep:[],consolidate:[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Merge'}]});
    expect(()=>validateCuratorResult(value,[memory('one'),memory('two',extra)],[])).toThrow();
  });
  it.each([
    {...replacement,statement:'api_key=sk-example-secret-value-123456789'},
    {...replacement,sensitivity:'restricted'},
    {...replacement,valid_from:'2026-02-30'},
    {...replacement,valid_from:'2026-12-01',valid_until:'2026-01-01'}
  ])('rejects unsafe replacement content and dates %j',value=>{
    expect(()=>validateCuratorResult(plan({keep:[],update:[{id:'M1',memory:value,source_refs:['M1'],reason:'Clarify'}]}),[memory('one')],[])).toThrow();
  });
  it('cannot absorb restricted context into ordinary knowledge',()=>{
    const value=plan({keep:[],update:[{id:'M1',memory:replacement,source_refs:['M1','M2'],reason:'Clarify'}]});
    expect(()=>validateCuratorResult(value,[memory('one')],[memory('two',{sensitivity:'restricted'})])).toThrow(/ineligible/);
  });
  it('cannot promote a session-bound cancellation merely by rewriting its content',()=>{
    const value=plan({keep:[],update:[{id:'M1',memory:{...replacement,scope:'global'},source_refs:['M1'],reason:'Rewrite'}]});
    expect(()=>validateCuratorResult(value,[memory('one',{scope:'session',scope_key:'s1',data:'The agent cancelled this response.'})],[])).toThrow();
  });
});
