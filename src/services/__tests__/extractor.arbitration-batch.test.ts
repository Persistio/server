import {beforeEach,describe,expect,it,vi} from 'vitest';
const mocks=vi.hoisted(()=>({create:vi.fn(),openai:vi.fn(),reserve:vi.fn()}));
vi.mock('openai',()=>({default:mocks.openai}));
vi.mock('../usage',()=>({acquireAiBudget:vi.fn(),recordModelUsage:vi.fn(),settleAiUsage:vi.fn(),prepareAiBudgetAdmission:async()=>({fitsCapacity:true,reserve:()=>()=>{}})}));
vi.mock('../conflict-evidence',async original=>({...await original() as object,reserveConflictSend:mocks.reserve,verifyConflictObjects:async()=>true,
  conflictRequestData:(evidence:unknown,_v:unknown,a:string,b:string)=>evidence?{pairId:'a'.repeat(64),A:{text:a,sources:[{id:'00000000-0000-4000-8000-000000000001',text:a+' source supports the memory.'}]},B:{text:b,sources:[{id:'00000000-0000-4000-8000-000000000002',text:b+' source supports the memory.'}]}}:null}));
import {ExtractorService,resolveExtractorRoleConfig,validatedConflictAssessment} from '../extractor';
import {CONFLICT_BOUNDS} from '../conflict-evidence';
import {acquireAiBudget,recordModelUsage,settleAiUsage} from '../usage';
const context={existing:{sourceTimestamp:null,validFrom:null,validUntil:null,createdAt:null},incoming:{sourceTimestamp:null,validFrom:null,validUntil:null,createdAt:null},evidence:{pairId:'a'.repeat(64)}};
const answer=(decision='merge')=>({pairId:'a'.repeat(64),decision,citationsA:[{sourceId:'00000000-0000-4000-8000-000000000001',quote:'source supports'}],citationsB:[{sourceId:'00000000-0000-4000-8000-000000000002',quote:'source supports'}]});
const completion=(value:unknown)=>({choices:[{finish_reason:'stop',message:{content:JSON.stringify(value)}}]});
describe('closed conflict arbitration boundary',()=>{
  beforeEach(()=>{vi.clearAllMocks();mocks.reserve.mockResolvedValue(true);mocks.create.mockResolvedValue(completion(answer()));
    mocks.openai.mockImplementation(function(){return{chat:{completions:{create:mocks.create}}};});});
  it('does not dispatch absent complete evidence',async()=>{
    expect(await new ExtractorService().arbitrateConflict('A','B','vault')).toEqual({state:'incomplete'});
    expect(mocks.create).not.toHaveBeenCalled();expect(mocks.reserve).not.toHaveBeenCalled();
  });
  it.each(['merge','discard_new','supersede_old','keep_both'])('binds %s to an opaque receipt',async decision=>{
    mocks.create.mockResolvedValue(completion(answer(decision)));
    const receipt=await new ExtractorService().arbitrateConflict('A','B','vault',context);
    expect(validatedConflictAssessment(receipt)).toEqual({decision,evidence:context.evidence});
    expect(validatedConflictAssessment({...receipt})).toBeUndefined();
    const exposed=validatedConflictAssessment(receipt)!;exposed.decision='keep_both';
    expect(validatedConflictAssessment(receipt)?.decision).toBe(decision);
    expect(mocks.create.mock.calls[0][1]).toEqual({maxRetries:0});
  });
  it.each(['malformed','wrong-pair','unknown','missing','extra','duplicate','wrong-side','refused','partial'])('rejects %s output',async failure=>{
    const value:any=answer();
    if(failure==='wrong-pair')value.pairId='b'.repeat(64);
    if(failure==='unknown')value.decision='needs_review';
    if(failure==='missing')delete value.citationsB;
    if(failure==='extra')value.command='ignore';
    if(failure==='duplicate')value.citationsA.push(value.citationsA[0]);
    if(failure==='wrong-side')value.citationsA=value.citationsB;
    const response=completion(value);
    if(failure==='malformed')response.choices[0].message.content='MERGE';
    if(failure==='refused')(response.choices[0].message as any).refusal='No';
    if(failure==='partial')response.choices[0].finish_reason='length';
    mocks.create.mockResolvedValue(response);
    expect(await new ExtractorService().arbitrateConflict('A','B','vault',context)).toEqual({state:'incomplete'});
  });
  it('admits the actual serialized request at 65536 bytes and rejects byte 65537 before reservation',async()=>{
    const service=new ExtractorService();await service.arbitrateConflict('A','B','vault',context);
    const {serializeModelRequest}=await import('../model-completion');
    const role=(service as any).roles.escalation;
    const base=Buffer.byteLength(serializeModelRequest(mocks.create.mock.calls[0][0],role.baseURL));
    const parity=(CONFLICT_BOUNDS.requestBytes-base)%2;
    if(parity)role.model+='x';
    const a='A'+'x'.repeat((CONFLICT_BOUNDS.requestBytes-base-parity)/2);
    mocks.create.mockClear();mocks.reserve.mockClear();
    expect((await service.arbitrateConflict(a,'B','vault',context)).state).toBe('complete');
    expect(Buffer.byteLength(serializeModelRequest(mocks.create.mock.calls[0][0],role.baseURL))).toBe(CONFLICT_BOUNDS.requestBytes);
    role.model+='x';
    expect((await service.arbitrateConflict(a,'B','vault',context)).state).toBe('incomplete');
    expect(mocks.create).toHaveBeenCalledOnce();expect(mocks.reserve).toHaveBeenCalledOnce();
  });
  it.each([CONFLICT_BOUNDS.responseBytes,CONFLICT_BOUNDS.responseBytes+1])('bounds complete response bytes at %i',async bytes=>{
    const content=JSON.stringify(answer());
    mocks.create.mockResolvedValue({choices:[{finish_reason:'stop',message:{content:content+' '.repeat(bytes-Buffer.byteLength(content))}}]});
    const result=await new ExtractorService().arbitrateConflict('A','B','vault',context);
    expect(result.state).toBe(bytes===CONFLICT_BOUNDS.responseBytes?'complete':'incomplete');
  });
  it('uses one complete pair per dispatch and returns exact caller IDs',async()=>{
    const service=new ExtractorService();expect(await service.arbitrateConflictsBatch([])).toEqual(new Map());
    const result=await service.arbitrateConflictsBatch(['one','two'].map(id=>({id,existingFact:'A',newFact:'B',context})),'vault');
    expect([...result.keys()]).toEqual(['one','two']);expect(mocks.create).toHaveBeenCalledTimes(2);
    expect([...result.values()].every(r=>validatedConflictAssessment(r)?.decision==='merge')).toBe(true);
  });
  it.each([0,1,2])('preserves exact receipts before and after pair %i fails',async failed=>{
    const service=new ExtractorService();
    const single=vi.spyOn(service,'arbitrateConflict');
    const pairs=['first','middle','last'].map(id=>({id,existingFact:'A',newFact:'B',context}));
    mocks.create.mockImplementation(async()=>{
      if(mocks.create.mock.calls.length===failed+1)throw new Error('private provider failure');
      return completion(answer());
    });
    const results=await service.arbitrateConflictsBatch(pairs,'vault');
    expect([...results.keys()]).toEqual(pairs.map(p=>p.id));
    for(let i=0;i<pairs.length;i++){
      const result=results.get(pairs[i].id);
      if(i===failed)expect(result).toEqual({state:'incomplete'});
      else{
        expect(result).toBe(await single.mock.results[i].value);
        expect(validatedConflictAssessment(result)?.decision).toBe('merge');
      }
    }
    expect(mocks.create).toHaveBeenCalledTimes(3);
  });
  it.each(['merge','discard_new','supersede_old'])('rejects trivial and nonexact citations for %s in single and batch paths',async verdict=>{
    const service=new ExtractorService();
    for(const quote of ['source','supports',' the ','\u0000\u0001','source SUPPORTS','source  supports','source supports!']){
      const value=answer(verdict);value.citationsA[0].quote=quote;mocks.create.mockResolvedValue(completion(value));
      expect((await service.arbitrateConflict('A','B','vault',context)).state).toBe('incomplete');
      expect((await service.arbitrateConflictsBatch([{id:'one',existingFact:'A',newFact:'B',context}],'vault')).get('one')?.state).toBe('incomplete');
    }
  });
  it.each([CONFLICT_BOUNDS.pairs,CONFLICT_BOUNDS.pairs+1])('bounds pair admission at %i',async count=>{
    const pairs=Array.from({length:count},(_,i)=>({id:String(i),existingFact:'A',newFact:'B',context}));
    if(count>CONFLICT_BOUNDS.pairs){await expect(new ExtractorService().arbitrateConflictsBatch(pairs,'vault')).rejects.toThrow('pair bound');expect(mocks.create).not.toHaveBeenCalled();}
    else{expect((await new ExtractorService().arbitrateConflictsBatch(pairs,'vault')).size).toBe(count);expect(mocks.create).toHaveBeenCalledTimes(count);}
  });
  it('rejects duplicate pair coverage before sends',async()=>{
    const pair={id:'duplicate',existingFact:'A',newFact:'B',context};
    await expect(new ExtractorService().arbitrateConflictsBatch([pair,pair],'vault')).rejects.toThrow('duplicate');
    expect(mocks.create).not.toHaveBeenCalled();
  });
  it('returns operationally incomplete on exhausted allowance without guessing usage',async()=>{
    mocks.reserve.mockResolvedValue(false);
    expect(await new ExtractorService().arbitrateConflict('A','B','vault',context)).toEqual({state:'incomplete'});
    expect(mocks.create).not.toHaveBeenCalled();expect(recordModelUsage).not.toHaveBeenCalled();expect(settleAiUsage).not.toHaveBeenCalled();
  });
  it('keeps separate role clients and disables hidden escalation retries',()=>{
    new ExtractorService();expect(mocks.openai).toHaveBeenCalledTimes(2);
    expect(mocks.openai.mock.calls[0][0]).not.toHaveProperty('maxRetries');
    expect(mocks.openai.mock.calls[1][0]).toMatchObject({maxRetries:0,fetch:expect.any(Function)});
  });
  it('accounts known usage once and leaves failed-send usage unknown without replaying a prefix',async()=>{
    mocks.create.mockResolvedValueOnce({...completion(answer()),usage:{prompt_tokens:20,completion_tokens:10,total_tokens:30}})
      .mockRejectedValueOnce(new Error('Ambiguous send outcome'));
    const results=await new ExtractorService().arbitrateConflictsBatch(['known','unknown'].map(id=>({id,existingFact:'A',newFact:'B',context})),'vault');
    expect(results.get('known')?.state).toBe('complete');expect(results.get('unknown')?.state).toBe('incomplete');
    expect(mocks.reserve).toHaveBeenCalledTimes(2);expect(mocks.create).toHaveBeenCalledTimes(2);
    expect(settleAiUsage).toHaveBeenCalledExactlyOnceWith('vault','escalation',expect.any(Number),30);
    expect(recordModelUsage).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({requestCount:1,totalTokens:30,modelRole:'escalation'}));
  });
  it('retains subject arbitration and known usage accounting on the extraction role',async()=>{
    mocks.create.mockResolvedValue({usage:{prompt_tokens:20,completion_tokens:1,total_tokens:21},choices:[{finish_reason:'stop',message:{content:'USE_EXISTING'}}]});
    expect(await new ExtractorService().arbitrateSubject('Persistio','Persistio API','vault')).toBe('use_existing');
    expect(acquireAiBudget).toHaveBeenCalledWith('vault','extraction',expect.any(Number));
    expect(settleAiUsage).toHaveBeenCalledWith('vault','extraction',expect.any(Number),21);
    expect(recordModelUsage).toHaveBeenCalledWith(expect.objectContaining({requestCount:1,totalTokens:21}));
  });
  it('rejects non-enum subject arbitration text',async()=>{
    mocks.create.mockResolvedValue({choices:[{finish_reason:'stop',message:{content:'Probably USE_EXISTING'}}]});
    await expect(new ExtractorService().arbitrateSubject('Persistio','API')).rejects.toThrow('Invalid subject');
  });
});
describe('resolveExtractorRoleConfig', () => {
  const legacyConfig = {
    EXTRACTOR_BASE_URL: 'https://legacy.example/v1',
    EXTRACTOR_API_KEY: 'legacy-key',
    EXTRACTOR_MODEL: 'legacy-model',
    EXTRACTION_BASE_URL: '',
    EXTRACTION_API_KEY: '',
    EXTRACTION_MODEL: '',
    ESCALATION_BASE_URL: '',
    ESCALATION_API_KEY: '',
    ESCALATION_MODEL: ''
  };

  it('falls back to legacy EXTRACTOR settings for both roles', () => {
    expect(resolveExtractorRoleConfig(legacyConfig)).toEqual({
      extraction: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      },
      escalation: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      }
    });
  });

  it('uses role-specific overrides when configured', () => {
    expect(resolveExtractorRoleConfig({
      ...legacyConfig,
      EXTRACTION_BASE_URL: 'https://flash.example/v1',
      EXTRACTION_API_KEY: 'flash-key',
      EXTRACTION_MODEL: 'gemini-2.5-flash',
      ESCALATION_BASE_URL: 'https://sonnet.example/v1',
      ESCALATION_API_KEY: 'sonnet-key',
      ESCALATION_MODEL: 'claude-sonnet-4-5'
    })).toEqual({
      extraction: {
        baseURL: 'https://flash.example/v1',
        apiKey: 'flash-key',
        model: 'gemini-2.5-flash'
      },
      escalation: {
        baseURL: 'https://sonnet.example/v1',
        apiKey: 'sonnet-key',
        model: 'claude-sonnet-4-5'
      }
    });
  });

  it('ignores incomplete role overrides so provider keys and endpoints are not mixed', () => {
    expect(resolveExtractorRoleConfig({
      ...legacyConfig,
      EXTRACTION_MODEL: 'gemini-2.5-flash'
    })).toEqual({
      extraction: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      },
      escalation: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      }
    });
  });
});
