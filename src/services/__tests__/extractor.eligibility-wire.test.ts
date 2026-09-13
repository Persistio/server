import OpenAI from 'openai';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { getConfig } from '../../config';
import { ExtractorService } from '../extractor';
import { acquireAiBudget, recordModelUsage, settleAiUsage } from '../usage';

vi.mock('../usage',()=>({acquireAiBudget:vi.fn(),recordModelUsage:vi.fn(),settleAiUsage:vi.fn()}));

const config=getConfig();
const original={EXTRACTION_BASE_URL:config.EXTRACTION_BASE_URL,EXTRACTION_API_KEY:config.EXTRACTION_API_KEY,EXTRACTION_MODEL:config.EXTRACTION_MODEL};
afterEach(()=>{Object.assign(config,original);vi.clearAllMocks();});

describe('internal extraction eligibility reaches the actual provider request',()=>{
  it.each([
    ['https://generativelanguage.googleapis.com/v1beta/openai/','gemini-2.5-flash',true],
    ['https://generativelanguage.googleapis.com/v1beta/openai/','gemini-2.5-flash',false],
    ['https://api.anthropic.com/v1/','claude-sonnet-4-6',true],
    ['https://api.anthropic.com/v1/','claude-sonnet-4-6',false]
  ] as const)('dispatches and budgets %s (%s), human=%s',async(baseURL,model,humanIntentAvailable)=>{
    Object.assign(config,{EXTRACTION_BASE_URL:baseURL,EXTRACTION_API_KEY:'synthetic-key',EXTRACTION_MODEL:model});
    const native=baseURL.includes('anthropic');
    const response=native
      ? {type:'message',role:'assistant',stop_reason:'end_turn',content:[{type:'text',text:'{"facts":[]}'}],usage:{input_tokens:17,output_tokens:11}}
      : {choices:[{finish_reason:'stop',message:{content:'{"facts":[]}'}}],usage:{prompt_tokens:17,completion_tokens:11,total_tokens:28}};
    const fetch=vi.fn(async(_url:unknown,_init?:RequestInit)=>new Response(JSON.stringify(response),{headers:{'content-type':'application/json'}}));
    const service=new ExtractorService();
    // Keep the real SDK's serialization/auth/transport. Inject only its fetch so
    // this test cannot call a paid provider.
    (service as unknown as {roles:{extraction:{client:OpenAI}}}).roles.extraction.client=
      new OpenAI({apiKey:'synthetic-key',baseURL,maxRetries:0,fetch:fetch as typeof globalThis.fetch});
    await expect(service.extractFacts('Synthetic evidence',undefined,'synthetic-vault',undefined,{humanIntentAvailable})).resolves.toEqual([]);
    expect(fetch).toHaveBeenCalledTimes(1);
    const [url,init]=fetch.mock.calls[0];
    expect(String(url)).toBe(`${baseURL}${native?'messages':'chat/completions'}`);
    const wire=String(init!.body);
    const body=JSON.parse(wire);
    const schema=native?body.output_config.format.schema:body.response_format.json_schema.schema;
    expect(schema.properties.facts.items.properties.type.enum).toHaveLength(humanIntentAvailable?9:7);
    expect(schema.properties.facts.items.properties.type.enum.includes('user_preference')).toBe(humanIntentAvailable);
    expect(schema.properties.facts.items.properties.type.enum.includes('user_rule')).toBe(humanIntentAvailable);
    expect(body).not.toHaveProperty('humanIntentAvailable');
    expect(wire).toBe(JSON.stringify(body,null,2));
    expect(acquireAiBudget).toHaveBeenCalledExactlyOnceWith('synthetic-vault','extraction',Math.max(256,Math.ceil(wire.length/4)));
    expect(settleAiUsage).toHaveBeenCalledExactlyOnceWith('synthetic-vault','extraction',Math.max(256,Math.ceil(wire.length/4)),28);
    expect(recordModelUsage).toHaveBeenCalledWith(expect.objectContaining({model,promptTokens:17,completionTokens:11,totalTokens:28}));
  });
});
