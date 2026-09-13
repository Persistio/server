import crypto from 'node:crypto';
import Fastify,{type FastifyRequest} from 'fastify';
import {beforeEach,describe,it,expect,vi} from 'vitest';
const state=vi.hoisted(()=>({query:vi.fn(),embed:vi.fn(),quota:vi.fn(),premium:true,semantic:[] as any[],graph:[] as any[],
  final:undefined as undefined|((rows:any[])=>any[]),counterFails:false,counterWait:undefined as undefined|Promise<unknown>,sourceRows:[] as any[],storage:vi.fn()}));
vi.mock('../middleware/auth',()=>({requireVaultReadAuth:async(r:FastifyRequest)=>{
  r.vault={id:'11111111-1111-4111-8111-111111111111',encrypted_dek:null,vault_encryption_enabled:false} as any;
}}));
vi.mock('../config',()=>({getConfig:()=>({DEFAULT_RECALL_TOP_K:5,MIN_RECALL_SIMILARITY:0.3})}));
vi.mock('../db/client',()=>({query:state.query}));
vi.mock('../services/embedder',()=>({getEmbedder:()=>({embed:state.embed})}));
vi.mock('../services/crypto',()=>({prepareVaultCrypto:async()=>({decrypt:(_v:unknown,s:string)=>s})}));
vi.mock('../services/usage',()=>({consumeApiQuota:state.quota,applyRateLimitHeaders:vi.fn()}));
vi.mock('../services/curation-capacity',()=>({getCuratorLimits:async()=>({curator_enabled:state.premium})}));
vi.mock('../services/raw-chunk-storage',()=>({getRawChunkStorage:()=>({store:'local',get:state.storage})}));
import {registerRecallRoutes,recallCandidateLimit,RECALL_COUNTER_UPDATE_SQL} from './recall';
function row(extra:Record<string,unknown>={}){
  const now=new Date().toISOString();
  return{id:crypto.randomUUID(),data:'A useful fact',subject:'Topic',subject_encrypted:null,categories:[],type:'system_fact',
    scope:'session',scope_key:'s1',status:'active',archived_at:null,confidence:0.9,salience:0.8,sensitivity:'low',polarity:'neutral',score:8,
    valid_from:null,valid_until:null,source_timestamp:now,source_segment_id:null,source_chunks:[],revision:'1',created_at:now,updated_at:now,
    similarity:0.9,source:'semantic',...extra};
}
async function request(payload:Record<string,unknown>={},format=''){
  const app=Fastify();await registerRecallRoutes(app);
  try{return await app.inject({method:'POST',url:'/v1/recall'+format,payload:{query:'Useful query',context:{session_id:'s1'},...payload}});}
  finally{await app.close();}
}
describe('server-owned recall composition',()=>{
  beforeEach(()=>{
    vi.clearAllMocks();state.premium=true;state.semantic=[];state.graph=[];state.final=undefined;state.counterFails=false;state.counterWait=undefined;state.sourceRows=[];
    state.embed.mockResolvedValue([1,0]);state.quota.mockResolvedValue({});state.storage.mockResolvedValue('Source text');
    state.query.mockImplementation(async(sql:string,args:any[])=>{
      if(sql.includes("'semantic' AS source"))return{rows:state.semantic.filter(m=>m.similarity>=args[6])};
      if(sql.includes("'graph' AS source"))return{rows:state.graph.filter(m=>!args[5].includes(m.id)).slice(0,args[6])};
      if(sql.includes('WITH selected AS')){
        const selected=JSON.parse(args[5]),all=[...state.semantic,...state.graph];
        const rows=selected.flatMap((r:any)=>{const m=all.find(m=>m.id===r.id&&m.revision===r.revision);return m?[{...m,source:r.source,similarity:r.similarity}]:[];});
        return{rows:state.final?state.final(rows):rows};
      }
      if(sql===RECALL_COUNTER_UPDATE_SQL){
        if(state.counterWait)return state.counterWait;
        if(state.counterFails)throw Error('Synthetic counter failure');return{rowCount:1,rows:[]};
      }
      if(sql.includes('FROM raw_chunks'))return{rows:state.sourceRows};
      throw Error('Unexpected query in recall test');
    });
  });
  it('bounds candidate overfetch independently of the response budget',()=>{
    expect([3,10,100].map(recallCandidateLimit)).toEqual([25,40,400]);
  });
  it('shares one total budget across semantic and entitled graph results without duplicate IDs',async()=>{
    const preference=row({scope:'global',scope_key:null,type:'user_preference'});
    const direct=row(),graph=row({source:'graph',similarity:null});
    state.semantic=[preference,direct];state.graph=[direct,graph];
    const res=await request({top_k:3});
    expect(res.statusCode,res.body).toBe(200);
    const body=res.json();expect(body.memories.map((r:any)=>r.id)).toEqual([preference.id,direct.id]);expect(body.related_memories.map((r:any)=>r.id)).toEqual([graph.id]);
  });
  it('never invents graph similarity and requires premium entitlement',async()=>{
    state.semantic=[row()];state.graph=[row({source:'graph',similarity:null})];
    expect((await request({top_k:2})).json().related_memories[0].similarity).toBeNull();
    state.premium=false;expect((await request({top_k:2})).json().related_memories).toEqual([]);
  });
  it.each(['agent','factual'] as const)('applies %s type preference only between close semantic matches',async mode=>{
    const preference=row({type:'user_preference',data:'Preference'}),fact=row({type:'system_fact',data:'Fact'});
    state.semantic=[preference,fact];
    expect((await request({mode,top_k:1})).json().memories[0].id).toBe(mode==='agent'?preference.id:fact.id);
    fact.similarity=0.99;preference.similarity=0.7;
    expect((await request({mode,top_k:1})).json().memories[0].id).toBe(fact.id);
  });
  it('uses actual source date for recency, not import time, without overcoming strong relevance',async()=>{
    const historical=row({source_timestamp:'2020-01-01T00:00:00Z'}),fresh=row();
    state.semantic=[historical,fresh];
    expect((await request({top_k:1})).json().memories[0].id).toBe(fresh.id);
    historical.similarity=0.99;fresh.similarity=0.6;
    expect((await request({top_k:1})).json().memories[0].id).toBe(historical.id);
  });
  it('does not fill top_k with below-threshold memories',async()=>{
    state.semantic=[row({similarity:0.9}),row({similarity:0.2})];
    expect((await request({top_k:5,min_similarity:0.5})).json().memories).toHaveLength(1);
  });
  it.each([
    {status:'superseded'},{archived_at:'2026-01-01'},{scope_key:'other-session'},{sensitivity:'restricted'},
    {confidence:0},{confidence:2},{source_timestamp:'2099-01-01T00:00:00Z'}
  ])('defensively excludes final ineligible rows %j',change=>{
    state.semantic=[row()];state.final=rows=>rows.map(m=>({...m,...change}));
    return request().then(res=>expect(res.json().memories).toEqual([]));
  });
  it('returns no stale selected memory when final database revision/identity snapshot excludes it',async()=>{
    state.semantic=[row()];state.final=()=>[];
    expect((await request()).json().memories).toEqual([]);
    expect(state.query.mock.calls.some(([s])=>s===RECALL_COUNTER_UPDATE_SQL)).toBe(false);
    const sql=state.query.mock.calls.find(([s])=>s.includes('WITH selected AS'))![0];
    expect(sql).toContain('m.revision=selected.revision');expect(sql).toContain('v.encrypted_dek IS NOT DISTINCT FROM');
    expect(sql).toContain('origin.revision=seed.revision');expect(sql).toContain("selected.source<>'graph'");
  });
  it.each([{valid_until:'2020-12-31'},{valid_from:'2099-01-01'}])('retains dated ordinary knowledge %j in JSON and labelled bundles',async dates=>{
    state.semantic=[row(dates)];
    expect((await request()).json().memories).toHaveLength(1);
    const value=(await request({},'?format=bundle_v3')).json();
    expect(value.bundle).toContain(dates.valid_from?'future':'historical');
  });
  it('never inserts an unrelated vault-wide rule ahead of relevant facts',async()=>{
    const rule=row({scope:'global',scope_key:null,type:'user_rule',similarity:0.1,salience:1});
    const fact=row();state.semantic=[rule,fact];
    expect((await request({top_k:1})).json().memories.map((m:any)=>m.id)).toEqual([fact.id]);
    rule.similarity=0.99;
    expect((await request({top_k:1,context:{session_id:'another'}})).json().memories.map((m:any)=>m.id)).toEqual([rule.id]);
  });
  it('returns only the server bundle contract and counts only records that fit',async()=>{
    state.semantic=[row({data:'A'.repeat(9000)}),row({data:'Short fact'})];
    const res=await request({max_bundle_bytes:1200},'?format=bundle_v3'),body=res.json();
    expect(Object.keys(body).sort()).toEqual(['bundle','schema_version']);expect(Buffer.byteLength(body.bundle)).toBeLessThanOrEqual(1200);
    const update=state.query.mock.calls.find(([s])=>s===RECALL_COUNTER_UPDATE_SQL)!;
    expect(update[1][1]).toEqual([state.semantic[1].id]);
  });
  it('retains uncertainty in each returned record without claiming model consumption',async()=>{
    state.semantic=[row({unresolved_conflict:true})];
    const res=await request({},'?format=bundle_v3');
    expect(res.json().bundle).toContain('Unresolved conflicting evidence');expect(res.json()).not.toHaveProperty('delivery');
  });
  it('does not expose encrypted storage fields or make a counter failure block recall',async()=>{
    state.semantic=[row({subject_encrypted:'Decoded subject'})];state.counterFails=true;
    const res=await request();expect(res.statusCode).toBe(200);expect(res.json().memories[0].subject).toBe('Decoded subject');
    expect(res.json().memories[0]).not.toHaveProperty('subject_encrypted');expect(res.json()).not.toHaveProperty('delivery');
  });
  it.each(['resolve','reject'] as const)('returns the complete bundle while counters remain pending, then tolerates %s',async outcome=>{
    let resolve!:(value:unknown)=>void, reject!:(error:Error)=>void;
    state.counterWait=new Promise((yes,no)=>{resolve=yes;reject=no;});
    const counterResult=state.counterWait.catch(()=>undefined);
    state.semantic=[row({data:'Durable knowledge returned before counter completion'})];
    try{
      const response=await request({},'?format=bundle_v3');
      expect(response.statusCode).toBe(200);
      expect(response.json().bundle).toContain(state.semantic[0].data);
      expect(response.json().bundle).toMatch(/<\/persistio_context>$/);
      expect(state.query.mock.calls.filter(([sql])=>sql===RECALL_COUNTER_UPDATE_SQL)).toHaveLength(1);
    }finally{
      if(outcome==='resolve')resolve({rowCount:1,rows:[]});
      else reject(new Error('Synthetic delayed counter failure'));
      await counterResult;
    }
  });
  it('enforces a single source-byte budget and skips wrong-store or oversized source objects',async()=>{
    const id=crypto.randomUUID();state.semantic=[row({source_chunks:[id]})];
    state.sourceRows=[
      {id:crypto.randomUUID(),blob_store:'other',blob_key:'wrong',storage_bytes:'1'},
      {id:crypto.randomUUID(),blob_store:'local',blob_key:'large',storage_bytes:'65537'},
      {id,blob_store:'local',blob_key:'first',storage_bytes:'40000',role:'user'},
      {id:crypto.randomUUID(),blob_store:'local',blob_key:'last',storage_bytes:'30000',role:'assistant'}
    ];state.storage.mockResolvedValue('x'.repeat(40000));
    const body=(await request({include_evidence:true,include_raw:true})).json();
    expect(body.evidence_chunks).toHaveLength(1);expect(body.raw_chunks).toEqual([]);
    expect(state.storage).toHaveBeenCalledExactlyOnceWith('first');
  });
});
