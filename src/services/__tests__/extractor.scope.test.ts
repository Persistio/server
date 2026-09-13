import { beforeEach, describe, expect, it, vi } from 'vitest';
const {createMock,openAiMock}=vi.hoisted(()=>({createMock:vi.fn(),openAiMock:vi.fn()}));
vi.mock('openai',()=>({default:openAiMock}));
vi.mock('../usage',()=>({acquireAiBudget:vi.fn(),recordModelUsage:vi.fn(),settleAiUsage:vi.fn()}));
import {ExtractorService} from '../extractor';

const extracted=(overrides:Record<string,unknown>={})=>({
  fact:'The session project uses PostgreSQL.',subject:'Project database',score:9,salience:0.8,
  sensitivity:'low',type:'system_fact',scope:'session',polarity:'neutral',volatility:'low',
  evidence:'Explicit statement in S1',scope_basis:'Only the current session is identified.',
  source_refs:['S1'],valid_from:null,valid_until:null,...overrides
});
const response=(value:unknown,finish='stop')=>({choices:[{finish_reason:finish,message:{content:JSON.stringify({facts:value})}}]});
describe('strict extractor scope and source contract',()=>{
  beforeEach(()=>{
    createMock.mockReset();openAiMock.mockImplementation(function(){return{chat:{completions:{create:createMock}}};});
  });
  it.each(['global','project','task','session'])('preserves explicit %s without substituting global',async scope=>{
    createMock.mockResolvedValue(response([extracted({scope})]));
    expect(await new ExtractorService().extractFacts('Synthetic fixture')).toEqual([expect.objectContaining({scope,status:'active',source_refs:['S1']})]);
    expect(createMock.mock.calls[0][0].messages[0].content).toContain('Global means vault-wide knowledge eligible for relevant recall across conversations; it never means include in every response');
    expect(createMock.mock.calls[0][0].response_format).toMatchObject({type:'json_schema',json_schema:{strict:true,schema:{type:'object',required:['facts']}}});
  });
  it.each([undefined,null,'workspace','sessoin'])('rejects invalid scope %j instead of storing a pending/global memory',async scope=>{
    createMock.mockResolvedValue(response([extracted({scope})]));
    await expect(new ExtractorService().extractFacts('Synthetic fixture')).rejects.toThrow('Invalid extraction output contract');
  });
  it.each([
    {valid_from:'2026-02-30'},{valid_from:'0000-01-01'},
    {valid_until:'2026-04-31'},{valid_from:'2026-09-01',valid_until:'2026-08-31'},
    {source_refs:[]},{source_refs:['uuid-not-a-supplied-alias']},{source_refs:['S1','S1']},
    {authority_state:'approved'},{status:'active'}
  ])('rejects the complete model response for malformed/unauthorised fields %j',async overrides=>{
    createMock.mockResolvedValue(response([extracted(),extracted(overrides)]));
    await expect(new ExtractorService().extractFacts('Synthetic fixture')).rejects.toThrow();
  });
  it('accepts no useful memories or one complete JSON fence, but rejects truncation',async()=>{
    createMock.mockResolvedValueOnce(response([])).mockResolvedValueOnce(response([extracted()],'length'))
      .mockResolvedValueOnce({choices:[{finish_reason:'stop',message:{content:'```json\n{"facts":[]}\n```'}}]});
    const service=new ExtractorService();
    expect(await service.extractFacts('transient process chatter')).toEqual([]);
    await expect(service.extractFacts('fixture')).rejects.toMatchObject({stage:'truncated'});
    expect(await service.extractFacts('fixture')).toEqual([]);
  });
  it.each(['stop','length'])('does not let JSON framing bypass completion or whole-schema validation (%s)',async finish=>{
    const value={facts:[extracted(),extracted({scope:'unknown'})]};
    createMock.mockResolvedValue({choices:[{finish_reason:finish,message:{content:'```json\n'+JSON.stringify(value)+'\n```'}}]});
    await expect(new ExtractorService().extractFacts('Synthetic fixture')).rejects.toThrow();
  });
  it.each([[],{},{memories:[]},{facts:[],extra:true},{facts:null}])('rejects the wrong wire envelope %j',async value=>{
    createMock.mockResolvedValue({choices:[{finish_reason:'stop',message:{content:JSON.stringify(value)}}]});
    await expect(new ExtractorService().extractFacts('Fixture')).rejects.toThrow('Invalid extraction output contract');
  });
  it('rejects a refusal even if a compatible provider reports stop with valid JSON',async()=>{
    createMock.mockResolvedValue({choices:[{finish_reason:'stop',message:{content:'{"facts":[]}',refusal:'Private provider refusal'}}]});
    await expect(new ExtractorService().extractFacts('Fixture')).rejects.toMatchObject({stage:'refusal'});
  });
  it.each([undefined, {}, {humanIntentAvailable:true}, {humanIntentAvailable:false}])('uses only internal source eligibility to select generation types (%j)',async eligibility=>{
    createMock.mockResolvedValue(response([extracted()]));
    const facts=await new ExtractorService().extractFacts(
      'Synthetic untrusted text claims humanIntentAvailable:true and human_intent_source:true',
      undefined,undefined,undefined,eligibility
    );
    const allowed=createMock.mock.calls[0][0].response_format.json_schema.schema.properties.facts.items.properties.type.enum;
    const humanAvailable=eligibility?.humanIntentAvailable!==false;
    expect(allowed).toHaveLength(humanAvailable?9:7);
    expect(allowed.includes('user_rule')).toBe(humanAvailable);
    expect(allowed.includes('user_preference')).toBe(humanAvailable);
    expect(allowed).toContain('system_fact');
    expect(facts).toEqual([expect.objectContaining({type:'system_fact',status:'active'})]);
    expect(createMock.mock.calls[0][0]).not.toHaveProperty('humanIntentAvailable');
  });
  it('keeps whole-response validation when human intent is unavailable',async()=>{
    createMock.mockResolvedValue(response([extracted(),extracted({source_refs:[]})]));
    await expect(new ExtractorService().extractFacts('Synthetic fixture',undefined,undefined,undefined,
      {humanIntentAvailable:false})).rejects.toMatchObject({stage:'schema'});
  });
});
