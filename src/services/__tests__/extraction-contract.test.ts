import { describe, expect, it } from 'vitest';
import { admitExtractionCandidates, parseExtractedFacts, parseExtractionResponse, resolveExtractionSources, sourceHasHumanIntent, formatExtractionSources,
  extractionResponseSchema, nonHumanExtractionResponseSchema, memoryTypeSchema, type ExtractionSource } from '../extraction-contract';

const fact = {
  fact: 'Use UK English by default.', subject: 'Language preference', score: 8, salience: 0.8,
  sensitivity: 'low', type: 'user_preference', scope: 'global', polarity: 'positive', volatility: 'low',
  evidence: 'Explicit user preference', scope_basis: 'Cross-conversation preference', source_refs: ['S1'],
  valid_from: null, valid_until: null
};
const source: ExtractionSource = { id: 'source-1', role: 'user', content: 'Respond to me in UK English at all times',
  created_at: '2026-09-12T00:00:00Z', provenance: null, current: true };

describe('one source-grounded extraction contract', () => {
  it('excludes context-only and unsupported intent without losing independent facts', () => {
    const sources = [{...source,current:false}, {...source,role:'assistant'}];
    const facts = parseExtractedFacts([fact, {...fact,source_refs:['S2']},
      {...fact,type:'system_fact',source_refs:['S2']}]);
    expect(admitExtractionCandidates(facts,sources,{})).toEqual({
      accepted:[facts[2]],excluded:{context_only:1,unsupported_human_intent:1}
    });
    expect(admitExtractionCandidates(facts.slice(0,2),sources,{})).toEqual({
      accepted:[],excluded:{context_only:1,unsupported_human_intent:1}
    });
  });
  it.each([false,true])('preflights corrupt references, tool evidence and bindings before exclusions, reverse=%s', reverse => {
    const sources=[{...source,current:false},{...source,role:'tool',current:true}];
    for(const invalid of [{...fact,source_refs:['S99']},{...fact,scope:'task'},
      {...fact,source_refs:['S2']},{...fact,source_refs:['S1','S99']}]){
      const facts=parseExtractedFacts(reverse?[invalid,fact]:[fact,invalid]);
      expect(()=>admitExtractionCandidates(facts,sources,{})).toThrow();
    }
  });
  it('admits contextual short answers with both current and supporting human evidence', () => {
    const sources=[{...source,role:'assistant',current:false,content:'Which response style do you prefer?'},
      {...source,content:'Concise summaries.'}];
    const facts=parseExtractedFacts([{...fact,source_refs:['S1','S2']}]);
    expect(admitExtractionCandidates(facts,sources,{})).toEqual({
      accepted:facts,excluded:{context_only:0,unsupported_human_intent:0}
    });
    // Both exclusion predicates apply: count the proposal once, as context-only.
    expect(admitExtractionCandidates(facts,sources.map(s=>({...s,role:'assistant',current:false})),{}).excluded)
      .toEqual({context_only:1,unsupported_human_intent:0});
  });
  it('narrows only impossible human types in generation, preserving the complete local contract', () => {
    for (const type of memoryTypeSchema.options) {
      const envelope = { facts: [{ ...fact, type }] };
      expect(extractionResponseSchema.safeParse(envelope).success).toBe(true);
      expect(nonHumanExtractionResponseSchema.safeParse(envelope).success)
        .toBe(!['user_preference','user_rule'].includes(type));
    }
    for (const schema of [extractionResponseSchema, nonHumanExtractionResponseSchema]) {
      expect(schema.safeParse({ facts: [] }).success).toBe(true);
      for (const change of [{score:11},{valid_from:'2026-02-30'},
        {valid_from:'2026-10-10',valid_until:'2026-10-09'},{status:'active'},{source_refs:[]}]) {
        expect(schema.safeParse({facts:[{...fact,type:'system_fact',...change}]}).success).toBe(false);
      }
    }
    // The full local parser is not replaced by the narrower generation schema.
    // Resolve the claimed intent against its actual sources before any mutation.
    const candidate=parseExtractionResponse({facts:[fact]})[0];
    expect(()=>resolveExtractionSources(candidate,[{...source,role:'assistant'}],{})).toThrow('human source');
  });
  it('distinguishes wire delivery role from author eligibility without altering stored sources', () => {
    const agent:ExtractionSource={...source,content:'[Inter-session message] sourceSession=agent sourceChannel=internal sourceTool=sessions_send isUser=false\nStop this task.'};
    const routed:ExtractionSource={...source,role:'assistant',provenance:{actor_type:'human',authorship:'original',
      trigger_type:'delegated',artifact_type:'message',cadence:'one_off'}};
    const input=[agent,routed],before=structuredClone(input);
    const pack=JSON.parse(formatExtractionSources(input,{session_id:'s'}));
    expect(pack.sources).toEqual([
      expect.objectContaining({ref:'S1',transport_role:'user',human_intent_source:false}),
      expect.objectContaining({ref:'S2',transport_role:'assistant',human_intent_source:true})
    ]);
    expect(pack.sources.every((item:Record<string,unknown>)=>!Object.hasOwn(item,'role'))).toBe(true);
    expect(input).toEqual(before);
  });
  it('uses a single strict object-root model envelope without changing the worker array contract',()=>{
    expect(parseExtractionResponse({facts:[fact]})).toEqual([fact]);
    expect(parseExtractionResponse({facts:[]})).toEqual([]);
    for(const value of [[],[fact],{},{memories:[fact]},{facts:[],extra:true},{facts:Array(101).fill(fact)}]){
      expect(()=>parseExtractionResponse(value)).toThrow('Invalid extraction output contract');
    }
    expect(()=>parseExtractionResponse({facts:[fact,{...fact,type:'event'}]})).toThrow('facts.[].type:invalid_enum_value');
    expect(()=>parseExtractionResponse({facts:[{...fact,source_refs:['S1','S1']}]})).toThrow('Duplicate');
  });
  it('accepts explicit global preference and all supported bound scopes', () => {
    const validated = parseExtractedFacts([fact])[0];
    expect(resolveExtractionSources(validated, [source], {})).toEqual([source]);
    for (const scope of ['session','project','task'] as const) {
      const scoped = parseExtractedFacts([{ ...fact,scope }])[0];
      expect(resolveExtractionSources(scoped,[source],{ session_id: 's',project_id: 'p',task_id: 't' })).toEqual([source]);
      expect(() => resolveExtractionSources(scoped,[source],{})).toThrow('binding');
    }
  });
  it('rejects missing/unknown scope, lifecycle output, malformed dates and partial output as a whole', () => {
    for (const change of [{ scope: undefined }, { scope: 'universal' }, { status: 'active' },
      { score: '8' }, { salience: Infinity }, { valid_from: '2026-02-30' },
      { valid_from: '2026-12-01', valid_until: '2026-01-01' }, { fact: '' }]) {
      expect(() => parseExtractedFacts([fact,{ ...fact,...change }])).toThrow('contract');
    }
    expect(() => parseExtractedFacts({ memories: [] })).toThrow();
    expect(parseExtractedFacts([])).toEqual([]);
  });
  it('requires real current source references, not every chunk or old history alone', () => {
    const validated = parseExtractedFacts([fact])[0];
    expect(() => resolveExtractionSources(validated,[{ ...source,current: false }],{})).toThrow('current evidence');
    expect(() => resolveExtractionSources({ ...validated,source_refs: ['S2'] },[source],{})).toThrow();
    expect(() => parseExtractedFacts([{ ...fact,source_refs: ['S1','S1'] }])).toThrow('Duplicate');
    expect(() => resolveExtractionSources(validated,[{ ...source,role: 'tool' }],{})).toThrow();
  });
  it('allows supported assistant facts without manufacturing human preferences', () => {
    const assistant = { ...source,role: 'assistant',content: 'The service runs on port 4827.' };
    const validated = parseExtractedFacts([fact])[0];
    expect(() => resolveExtractionSources(validated,[assistant],{})).toThrow('human source');
    expect(resolveExtractionSources({ ...validated,type: 'system_fact' },[assistant],{})).toEqual([assistant]);
  });
  it('does not treat agent session routing as a blanket hold on a real human', () => {
    const routed = { ...source,provenance: { actor_type: 'human',trigger_type: 'delegated',artifact_type: 'message',
      authorship: 'original',cadence: 'one_off',source_class: 'agent_slack' } };
    expect(sourceHasHumanIntent(routed)).toBe(true);
    expect(sourceHasHumanIntent({ ...routed,provenance: { ...routed.provenance,actor_type: 'agent',authorship: 'generated' } })).toBe(false);
    expect(sourceHasHumanIntent({ ...source,provenance: { invalid: true } })).toBe(false);
  });
});
