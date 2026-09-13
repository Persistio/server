import {describe,it,expect} from 'vitest';
import {buildAliasMaps,validateCuratorResult,type CuratorMemory,type CuratorResult} from '../curator';
import {compileCuratorGraph} from '../curator-graph';
const memory=(id:string,extra:Partial<CuratorMemory>={}):CuratorMemory=>({id,subject:'Topic',data:'Supported durable fact',type:'system_fact',
  scope:'session',scope_key:'session',sensitivity:'low',salience:0.8,confidence:0.9,polarity:'neutral',volatility:'low',parent_id:null,...extra});
const replacement={statement:'Supported durable fact',subject:'Topic',type:'system_fact' as const,confidence:0.9,salience:0.8,
  sensitivity:'low' as const,polarity:'neutral' as const,volatility:'low' as const,valid_from:null,valid_until:null,evidence:'Equivalent sources'};
const plan=(count=0):CuratorResult=>({schema_version:'curation-plan.v2',keep:Array.from({length:count},(_,i)=>({id:'M'+(i+1),reason:'Useful'})),
  update:[],consolidate:[],archive:[],edges:[],scope_changes:[]});
const edge=(from:string,to:string,type:'supports'|'part_of'='supports')=>({from,to,type,confidence:0.8,reason:'Supported relation'});
const compiled=(p:CuratorResult,ms:CuratorMemory[])=>{validateCuratorResult(p,ms,[]);return compileCuratorGraph(p,ms,[],buildAliasMaps(ms,[]));};
describe('final-state active-memory graph compilation',()=>{
  it.each(['keep','update','archive','consolidate'] as const)('uses final survival after %s, not old identifier existence',disposition=>{
    const ms=[memory('one'),memory('two'),memory('three')],p=plan(3);
    if(disposition==='update'){p.keep=p.keep.slice(1);p.update=[{id:'M1',memory:{...replacement,subject:'Renamed'},source_refs:['M1'],reason:'Rename'}];}
    if(disposition==='archive'){p.keep=p.keep.slice(1);p.archive=[{id:'M1',basis:'no_durable_value',reason:'No durable value'}];}
    if(disposition==='consolidate'){p.keep=p.keep.slice(2);p.consolidate=[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Same information'}];}
    p.edges=[edge('M1','M3')];
    if(disposition==='keep'||disposition==='update')expect(compiled(p,ms).edges[0].from).toEqual({kind:'existing',id:'one'});
    else expect(()=>compiled(p,ms)).toThrow();
    if(disposition==='consolidate'){p.edges=[edge('N1','M3')];expect(compiled(p,ms).edges[0].from).toEqual({kind:'created',index:0});}
  });
  it.each(['Topic','one','30f02af7-ae15-4d69-914e-b7d121963283','C1','M99'])('rejects subject/raw/unseen reference %s without guessing',id=>{
    const p=plan(2);p.edges=[edge(id,'M2')];expect(()=>compiled(p,[memory('one'),memory('two')])).toThrow();
  });
  it('uses explicit aliases even when subjects collide or look like aliases',()=>{
    const p=plan(2);p.edges=[edge('M1','M2')];expect(compiled(p,[memory('one',{subject:'M2'}),memory('two',{subject:'M2'})]).edges).toHaveLength(1);
  });
  it('preserves and orders inherited parents across forward consolidation references',()=>{
    const ms=[memory('c1',{parent_id:'p1'}),memory('c2',{parent_id:'p1'}),memory('p1'),memory('p2')];
    const p=plan();p.consolidate=[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Child pair'},{id:'N2',sources:['M3','M4'],memory:replacement,reason:'Parent pair'}];
    expect(compiled(p,ms)).toMatchObject({creationOrder:[1,0],parents:[{kind:'created',index:1},null]});
  });
  it.each(['different-parents','unseen-parent','cross-binding','archived-parent'] as const)('rejects unsafe parent inheritance: %s',kind=>{
    const ms=[memory('c1',{parent_id:'p1'}),memory('c2',{parent_id:kind==='different-parents'?'p2':'p1'}),memory('p1'),memory('p2')];
    const p=plan();p.consolidate=[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Consolidate'}];p.keep=[{id:'M3',reason:'Useful'},{id:'M4',reason:'Useful'}];
    if(kind==='unseen-parent')ms[0].parent_id=ms[1].parent_id='outside';
    if(kind==='cross-binding')ms[2].scope_key='other';
    if(kind==='archived-parent'){p.keep=p.keep.slice(1);p.archive=[{id:'M3',basis:'duplicate',reason:'Retire'}];}
    expect(()=>compiled(p,ms)).toThrow();
  });
  it('rejects parent and part_of cycles but allows ordinary relationship cycles',()=>{
    const ms=[memory('one'),memory('two')],p=plan(2);p.edges=[edge('M1','M2'),edge('M2','M1')];expect(()=>compiled(p,ms)).not.toThrow();
    p.edges=[edge('M1','M2','part_of'),edge('M2','M1','part_of')];expect(()=>compiled(p,ms)).toThrow('cycle');
    p.edges=[];ms[0].parent_id='two';ms[1].parent_id='one';expect(()=>compiled(p,ms)).toThrow('cycle');
    ms[1].parent_id=null;p.edges=[edge('M2','M1','part_of')];expect(()=>compiled(p,ms)).toThrow('cycle');
  });
  it('rejects self, repeated and cross-binding edges',()=>{
    const ms=[memory('one'),memory('two')],p=plan(2);
    p.edges=[edge('M1','M1')];expect(()=>compiled(p,ms)).toThrow();
    p.edges=[edge('M1','M2'),edge('M1','M2')];expect(()=>compiled(p,ms)).toThrow();
    p.edges=[edge('M1','M2')];ms[1].scope_key='other';expect(()=>compiled(p,ms)).toThrow();
  });
  it('handles a bounded deep hierarchy and detects its closing cycle',()=>{
    const ms=Array.from({length:200},(_,i)=>memory('m'+i,{parent_id:i?'m'+(i-1):null})),p=plan(ms.length);
    expect(()=>compiled(p,ms)).not.toThrow();ms[0].parent_id='m199';expect(()=>compiled(p,ms)).toThrow('cycle');
  });
  it.each(['update','consolidate'] as const)('rejects disjoint %s evidence before embeddings/application',kind=>{
    const ms=[memory('one',{valid_from:'2026-09-01'}),memory('two',{valid_until:'2026-08-31'})],p=plan();
    if(kind==='update'){p.keep=[{id:'M2',reason:'Useful'}];p.update=[{id:'M1',memory:replacement,source_refs:['M1','M2'],reason:'Combine'}];}
    else p.consolidate=[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Combine'}];
    expect(()=>compiled(p,ms)).toThrow('temporal');
  });
});
