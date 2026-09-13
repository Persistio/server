import { describe,it,expect } from 'vitest';
import { buildAliasMaps, type CuratorMemory } from '../curator';
import { CURATOR_SCHEMA_VERSION,validateCuratorContract,type CuratorResult } from '../curator-contract';
import { compileCuratorGraph } from '../curator-graph';

const memory = (id:string,overrides:Partial<CuratorMemory>={}):CuratorMemory => ({
  id,subject:'Preferred English',data:'Use UK English',type:'user_preference',scope:'session',scope_key:'s1',
  salience:0.8,confidence:0.9,sensitivity:'low',polarity:'positive',volatility:'low',parent_id:null,
  row_version:'1',valid_from:null,valid_until:null,source_chunks:['source1'],...overrides
});
const proposal = {statement:'Use UK English',subject:'Preferred English',type:'user_preference' as const,
  confidence:0.9,salience:0.8,sensitivity:'low' as const,polarity:'positive' as const,volatility:'low' as const,
  valid_from:null,valid_until:null,evidence:'Explicit preference'};
const empty = ():CuratorResult => ({schema_version:CURATOR_SCHEMA_VERSION,keep:[],update:[],consolidate:[],archive:[],edges:[],scope_changes:[]});
const validate = (plan:unknown,targets=[memory('one')],context:CuratorMemory[]=[]) =>
  validateCuratorContract(plan,targets,context,buildAliasMaps(targets,context));

describe('active memory improvement contract',()=>{
  it('accepts keep as completed improvement without promotion',()=>{
    const p=empty();p.keep=[{id:'M1',reason:'Already useful'}];expect(validate(p)).toEqual(p);
  });
  it.each(['keep','update','consolidate','archive','edges','scope_changes'])('requires complete %s array',field=>{
    const p:any=empty();p.keep=[{id:'M1',reason:'Useful'}];delete p[field];expect(()=>validate(p)).toThrow();
  });
  it('rejects old candidate output and extra authority fields',()=>{
    expect(()=>validate({...empty(),promoted_candidates:[]})).toThrow();
    expect(()=>validate({...empty(),keep:[{id:'M1',reason:'Useful',approved:true}]})).toThrow();
  });
  it('requires exactly one primary disposition per selected target',()=>{
    expect(()=>validate(empty())).toThrow();
    const p=empty();p.keep=[{id:'M1',reason:'Useful'}];p.archive=[{id:'M1',reason:'Duplicate',basis:'duplicate'}];expect(()=>validate(p)).toThrow();
  });
  it('never resolves raw UUIDs or unknown aliases',()=>{
    const p=empty();p.keep=[{id:'one',reason:'Useful'}];expect(()=>validate(p)).toThrow();
    p.keep=[{id:'M2',reason:'Useful'}];expect(()=>validate(p)).toThrow();
  });
  it('allows explicitly reviewed context update and requires original supporting memory',()=>{
    const p=empty();p.keep=[{id:'M1',reason:'Useful'}];p.update=[{id:'M2',memory:proposal,source_refs:['M2'],reason:'Clarify'}];
    expect(validate(p,[memory('one')],[memory('two')])).toEqual(p);
    p.update[0].source_refs=['M1'];expect(()=>validate(p,[memory('one')],[memory('two')])).toThrow();
  });
  it.each([{scope:'task',scope_key:'t1'},{scope_key:'s2'},{valid_from:'2020-01-01'}, {sensitivity:'high'}] as Partial<CuratorMemory>[])
  ('rejects consolidation that loses binding, time or sensitivity: %j',override=>{
    const p=empty();p.consolidate=[{id:'N1',sources:['M1','M2'],memory:proposal,reason:'Same information'}];
    expect(()=>validate(p,[memory('one'),memory('two',override)])).toThrow();
  });
  it('rejects new behavioural intent derived only from factual inputs',()=>{
    const p=empty();p.update=[{id:'M1',memory:proposal,source_refs:['M1'],reason:'Rewrite'}];
    expect(()=>validate(p,[memory('one',{type:'system_fact'})])).toThrow();
  });
  it('does not offer uncertainty or expiry as an archive basis',()=>{
    expect(()=>validate({...empty(),archive:[{id:'M1',basis:'uncertain',reason:'Not sure'}]})).toThrow();
  });
  it('requires actual related raw sources for explicit global scope changes',()=>{
    const targets=[memory('one')],aliases=buildAliasMaps(targets,[]);
    const p=empty();p.keep=[{id:'M1',reason:'Useful'}];p.scope_changes=[{id:'M1',scope:'global',scope_key:null,source_refs:['S1'],reason:'Across all conversations'}];
    expect(()=>validateCuratorContract(p,targets,[],aliases)).toThrow();
    const raw={id:'source1',role:'user',content:'Respond to me in UK English at all times',provenance:null,created_at:'2026-01-01T00:00:00Z',current:true,context:{session_id:'s1'}};
    expect(validateCuratorContract(p,targets,[],aliases,[raw])).toEqual(p);
    expect(()=>validateCuratorContract(p,targets,[],aliases,[{...raw,id:'unrelated'}])).toThrow();
    expect(()=>validateCuratorContract(p,targets,[],aliases,[{...raw,role:'assistant'}])).toThrow();
  });
  it('uses surviving aliases for graph links, not retired source aliases or ambiguous subjects',()=>{
    const targets=[memory('one'),memory('two'),memory('three')],aliases=buildAliasMaps(targets,[]);
    const p=empty();p.consolidate=[{id:'N1',sources:['M1','M2'],memory:proposal,reason:'Same memory'}];p.keep=[{id:'M3',reason:'Distinct'}];
    p.edges=[{from:'N1',to:'M3',type:'supports',confidence:0.8,reason:'Related'}];
    expect(compileCuratorGraph(validateCuratorContract(p,targets,[],aliases),targets,[],aliases).edges).toHaveLength(1);
    p.edges[0].from='M1';expect(()=>validateCuratorContract(p,targets,[],aliases)).toThrow();
  });
  it('rejects graph cross-binding and hierarchy cycles before writes',()=>{
    const targets=[memory('one'),memory('two',{scope_key:'s2'})],aliases=buildAliasMaps(targets,[]);
    const p=empty();p.keep=[{id:'M1',reason:'Useful'},{id:'M2',reason:'Useful'}];
    p.edges=[{from:'M1',to:'M2',type:'part_of',confidence:1,reason:'Parent'}];
    expect(()=>compileCuratorGraph(validateCuratorContract(p,targets,[],aliases),targets,[],aliases)).toThrow();
    targets[1].scope_key='s1';p.edges.push({from:'M2',to:'M1',type:'part_of',confidence:1,reason:'Cycle'});
    expect(()=>compileCuratorGraph(validateCuratorContract(p,targets,[],aliases),targets,[],aliases)).toThrow();
  });
});
