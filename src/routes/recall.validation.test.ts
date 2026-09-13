import Fastify, { type FastifyRequest } from 'fastify';
import {beforeEach,describe,it,expect,vi} from 'vitest';
const mocks=vi.hoisted(()=>({quota:vi.fn(),embed:vi.fn(),query:vi.fn()}));
vi.mock('../middleware/auth',()=>({requireVaultReadAuth:async(request:FastifyRequest)=>{
  request.vault={id:'11111111-1111-4111-8111-111111111111',vault_encryption_enabled:false,encrypted_dek:null} as any;
}}));
vi.mock('../config',()=>({getConfig:()=>({DEFAULT_RECALL_TOP_K:5,MIN_RECALL_SIMILARITY:0.3})}));
vi.mock('../services/crypto',()=>({prepareVaultCrypto:async()=>({decrypt:(_v:unknown,s:string)=>s})}));
vi.mock('../services/usage',()=>({consumeApiQuota:mocks.quota,applyRateLimitHeaders:vi.fn()}));
vi.mock('../services/embedder',()=>({getEmbedder:()=>({embed:mocks.embed})}));
vi.mock('../db/client',()=>({query:mocks.query}));
vi.mock('../services/raw-chunk-storage',()=>({getRawChunkStorage:()=>({store:'local',get:vi.fn()})}));
import {registerRecallRoutes} from './recall';
describe('recall request boundary',()=>{
  beforeEach(()=>{vi.clearAllMocks();mocks.quota.mockResolvedValue({});mocks.embed.mockResolvedValue([1,0]);mocks.query.mockResolvedValue({rows:[],rowCount:0});});
  it.each(['','?format=bundle_v3'])('rejects invalid source/context combinations before work: %s',async suffix=>{
    const app=Fastify();await registerRecallRoutes(app);
    try{
      for(const field of ['include_raw','include_evidence']){
        for(const context of [{},{session_id:''},{session_id:' '},{session_id:'\ns'},{session_id:'s\t'},{session_id:'s'.repeat(513)}]){
          expect((await app.inject({method:'POST',url:'/v1/recall'+suffix,payload:{query:'fact',[field]:true,context}})).statusCode).toBe(400);
        }
        if(suffix)expect((await app.inject({method:'POST',url:'/v1/recall'+suffix,payload:{query:'fact',[field]:true,context:{session_id:'s1'}}})).statusCode).toBe(400);
      }
      for(const mock of Object.values(mocks))expect(mock).not.toHaveBeenCalled();
    }finally{await app.close();}
  });
  it.each(['bundle','bundle_v2','anything'])('rejects obsolete format %s before quota or provider calls',async format=>{
    const app=Fastify();await registerRecallRoutes(app);
    try{expect((await app.inject({method:'POST',url:'/v1/recall?format='+format,payload:{query:'fact'}})).statusCode).toBe(400);
      for(const mock of Object.values(mocks))expect(mock).not.toHaveBeenCalled();
    }finally{await app.close();}
  });
  it.each(['','?format=bundle_v3'])('serves non-raw requests and normalizes context: %s',async suffix=>{
    const app=Fastify();await registerRecallRoutes(app);
    try{
      for(const context of [{},{session_id:' session-a '}])expect((await app.inject({method:'POST',url:'/v1/recall'+suffix,payload:{query:'fact',context}})).statusCode).toBe(200);
      expect(mocks.quota).toHaveBeenCalledTimes(2);
      expect(mocks.query.mock.calls[1][1][1]).toBe('session-a');
    }finally{await app.close();}
  });
  it('binds source retrieval to normalized vault/session/project/task',async()=>{
    const app=Fastify();await registerRecallRoutes(app);
    try{
      expect((await app.inject({method:'POST',url:'/v1/recall',payload:{query:'fact',include_raw:true,context:{session_id:' s1 ',project_id:'p1',task_id:'t1'}}})).statusCode).toBe(200);
      const raw=mocks.query.mock.calls.find(([sql])=>sql.includes('FROM raw_chunks'))!;
      expect(raw[1][0]).toBe('11111111-1111-4111-8111-111111111111');expect(raw[1][1]).toBe('s1');
      expect(raw[1].slice(7)).toEqual(['p1','t1','local']);expect(raw[0]).toContain("capture_context->>'project_id'");
      expect(raw[0]).toContain('blob_store=$10');
    }finally{await app.close();}
  });
  it.each(['','?format=bundle_v3'])('rejects the removed global option before quota/provider calls: %s',async suffix=>{
    const app=Fastify();await registerRecallRoutes(app);
    try{
      for(const include_global_rules of [true,false]) {
        const res=await app.inject({method:'POST',url:'/v1/recall'+suffix,payload:{query:'fact',include_global_rules}});
        expect(res.statusCode,res.body).toBe(400);
      }
      for(const mock of Object.values(mocks))expect(mock).not.toHaveBeenCalled();
    }finally{await app.close();}
  });
});
