import {beforeEach,afterEach,describe,it,expect,vi} from 'vitest';
const mocks=vi.hoisted(()=>({create:vi.fn(),openai:vi.fn(),acquire:vi.fn(),settle:vi.fn(),record:vi.fn()}));
vi.hoisted(()=>{process.env.CURATOR_BASE_URL='https://curator.example/v1';process.env.CURATOR_API_KEY='test-curator-key';process.env.CURATOR_MODEL='test-curator-model';});
vi.mock('openai',()=>({default:mocks.openai}));
vi.mock('../usage',()=>({acquireAiBudget:mocks.acquire,recordModelUsage:mocks.record,settleAiUsage:mocks.settle}));
import {CuratorService,CURATOR_PROMPT_VERSION,type CuratorMemory} from '../curator';
import {CURATOR_CONTRACT,type CuratorResult} from '../curator-contract';
import {serializeModelRequest} from '../model-completion';
const memory=(id='target'):CuratorMemory=>({id,subject:'Service database',data:'The service uses PostgreSQL.',type:'system_fact',
  scope:'session',scope_key:'s1',salience:0.8,confidence:0.9,sensitivity:'low',polarity:'neutral',volatility:'low',parent_id:null});
const plan=(count=1):CuratorResult=>({schema_version:'curation-plan.v2',keep:Array.from({length:count},(_,i)=>({id:'M'+(i+1),reason:'Already useful'})),
  update:[],consolidate:[],archive:[],edges:[],scope_changes:[]});
const response=(value:unknown=plan(),finish='stop')=>({usage:{prompt_tokens:120,completion_tokens:30,total_tokens:150},
  choices:[{finish_reason:finish,message:{content:JSON.stringify(value)}}]});
describe('Curator model contract and paid-attempt accounting',()=>{
  beforeEach(()=>{Object.values(mocks).forEach(m=>m.mockReset());mocks.openai.mockImplementation(function(){return{chat:{completions:{create:mocks.create}}};});mocks.create.mockResolvedValue(response());});
  afterEach(()=>vi.restoreAllMocks());
  it('retains AI budget settlement and model billing with safe operational token counts',async()=>{
    const lines:string[]=[];vi.spyOn(console,'log').mockImplementation(value=>{lines.push(String(value));});
    await new CuratorService().curate([memory()],[],null,'vault-1');
    expect(mocks.acquire).toHaveBeenCalledWith('vault-1','curation',expect.any(Number));
    expect(mocks.settle).toHaveBeenCalledWith('vault-1','curation',expect.any(Number),150);
    expect(mocks.record).toHaveBeenCalledWith(expect.objectContaining({vaultId:'vault-1',provider:'curator.example',model:'test-curator-model',modelRole:'curation',
      source:'curation_worker',requestCount:1,promptTokens:120,completionTokens:30,totalTokens:150}));
    expect(lines.map(s=>JSON.parse(s))).toContainEqual(expect.objectContaining({event:'curator token usage',total_tokens:150,candidates_count:1}));
    expect(lines.join('')).not.toContain(memory().data);
  });
  it('records request before dispatch and returned usage even when a paid response is invalid',async()=>{
    const order:string[]=[];const accounting={beforeRequest:vi.fn(async()=>{order.push('request');}),returnedUsage:vi.fn(async()=>{order.push('usage');})};
    mocks.create.mockImplementation(async()=>{order.push('provider');return response({wrong:true});});
    const service=new CuratorService();
    await expect(service.curatePrepared(service.prepare([memory()],[],null),'vault-1',accounting)).rejects.toThrow('validation');
    expect(order).toEqual(['request','provider','usage']);expect(accounting.returnedUsage).toHaveBeenCalledWith({promptTokens:120,completionTokens:30,totalTokens:150});
  });
  it('does not dispatch after failed live entitlement/lease accounting',async()=>{
    const service=new CuratorService(),accounting={beforeRequest:vi.fn(async()=>{throw new Error('Live claim lost');}),returnedUsage:vi.fn()};
    await expect(service.curatePrepared(service.prepare([memory()],[],null),'vault-1',accounting)).rejects.toThrow('Live claim lost');
    expect(mocks.create).not.toHaveBeenCalled();expect(accounting.returnedUsage).not.toHaveBeenCalled();
  });
  it('does not invent token usage for a response without usage metadata',async()=>{
    mocks.create.mockResolvedValue({choices:response().choices});
    const accounting={beforeRequest:vi.fn(),returnedUsage:vi.fn()},service=new CuratorService();
    const result=await service.curatePrepared(service.prepare([memory()],[],null),'vault-1',accounting);
    expect(result.usage).toBeNull();expect(accounting.returnedUsage).not.toHaveBeenCalled();expect(mocks.settle).not.toHaveBeenCalled();
  });
  it.each([100,2000,8000,11999,12000.5,0,-1,Infinity,NaN])('does not call a model with unusable input capacity %s',async maxInputTokens=>{
    await expect(new CuratorService().curate([memory()],[],null,'vault-1',{maxInputTokens})).rejects.toThrow();
    expect(mocks.create).not.toHaveBeenCalled();
  });
  it('rejects a 2000-token budget rather than silently dropping custom instructions',async()=>{
    await expect(new CuratorService().curate([memory()],[],null,'vault-1',{
      maxInputTokens:2000,vaultPromptContext:{type:'custom',custom_curation_prompt:'Custom durable policy. '.repeat(4000)}
    })).rejects.toThrow();
    expect(mocks.create).not.toHaveBeenCalled();
    expect(mocks.acquire).not.toHaveBeenCalled();
  });
  it.each([undefined,12000])('preserves the exact mandatory contract and whole inputs with custom prompt under %s tokens',async cap=>{
    const service=new CuratorService();
    const batch=service.prepare([memory()],[],null,{maxInputTokens:cap,vaultPromptContext:{type:'custom',custom_curation_prompt:'Custom durable policy. '.repeat(4000)}});
    expect(String(batch.request.messages[0].content)).toContain(CURATOR_CONTRACT);
    expect(String(batch.request.messages[0].content)).toContain('Custom durable policy.');
    expect(String(batch.request.messages[0].content)).toContain('[truncated]');
    expect(serializeModelRequest(batch.request,'https://curator.example/v1').length).toBeLessThanOrEqual((cap??12000)*4-1000);
    expect(JSON.parse((batch.request.messages[1].content as any)[0].text).memories[0].statement).toBe(memory().data);
    await service.curatePrepared(batch,'vault-1');expect(mocks.create).toHaveBeenCalledWith(batch.request);
  });
  it('does not let a custom prompt suppress the server contract by naming its version',()=>{
    const batch=new CuratorService().prepare([memory()],[],null,{vaultPromptContext:{type:'custom',custom_curation_prompt:CURATOR_PROMPT_VERSION+' custom policy'}});
    expect(String(batch.request.messages[0].content)).toContain(CURATOR_CONTRACT);
  });
  it.each(['length','content_filter',undefined])('retains private validation audit and refuses incomplete response %s',async finish=>{
    mocks.create.mockResolvedValue({...response(),choices:[{finish_reason:finish,message:{content:'PRIVATE incomplete response'}}]});
    await expect(new CuratorService().curate([memory()],[],null,'vault-1')).rejects.toMatchObject({name:'CuratorPlanValidationError',
      audit:{schemaVersion:'curation-plan.v2',promptVersion:CURATOR_PROMPT_VERSION,rawResponse:expect.any(Object)}});
  });
  it('preserves explicit consolidation and update source aliases without promotion',async()=>{
    const replacement={statement:'The service uses PostgreSQL.',subject:'Database',type:'system_fact' as const,confidence:0.9,salience:0.8,sensitivity:'low' as const,
      polarity:'neutral' as const,volatility:'low' as const,valid_from:null,valid_until:null,evidence:'Equivalent supported facts'};
    const value=plan(0);value.consolidate=[{id:'N1',sources:['M1','M2'],memory:replacement,reason:'Equivalent facts'}];
    value.update=[{id:'M3',memory:replacement,source_refs:['M3'],reason:'Clarify wording'}];mocks.create.mockResolvedValue(response(value));
    const result=await new CuratorService().curate([memory('one'),memory('two'),memory('three')],[],null,'vault-1');
    expect(result.result).toEqual(value);expect(result.graph.creationOrder).toEqual([0]);
  });
});
