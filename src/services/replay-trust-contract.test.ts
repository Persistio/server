import { beforeAll, describe, expect, it } from 'vitest';
import { ingestSchema, bulkIngestSchema } from '../routes/ingest';
import { sourceHasHumanIntent, resolveExtractionSources, type ExtractionSource } from './extraction-contract';

let malformed:Array<{name:string;value:unknown}>;
let human:any;
let prepare:any;
beforeAll(async()=>{
  const fixture=await import(new URL('../../../../scripts/lib/replay-contract-cases.mjs',import.meta.url).href);
  malformed=fixture.malformedProvenanceCases();human=fixture.humanSource;
  ({prepareReplayDataset:prepare}=await import(new URL('../../../../scripts/lib/replay-dataset.mjs',import.meta.url).href));
});
const base={role:'user',content:'A supported historical statement.',timestamp:'2026-06-01T00:00:00Z'};
const source=(chunk:any):ExtractionSource=>({...chunk,id:'source',current:true,created_at:chunk.timestamp});
const options={datasetSha256:'a'.repeat(64),importJobId:'job'};
const segment=(chunk:any)=>({segment_id:'segment',session_id:'session',created_at:base.timestamp,chunks:[{...base,id:'one',...chunk}]});

describe('API and replay attribution under the active-memory contract',()=>{
  it('rejects malformed metadata at both APIs and never treats malformed history as human intent',()=>{
    for(const {name,value} of malformed){
      for(const schema of [ingestSchema,bulkIngestSchema]){
        expect(schema.safeParse({session_id:'session',chunks:[{...base,provenance:value}]}).success,name).toBe(false);
      }
      expect(sourceHasHumanIntent(source({...base,provenance:value})),name).toBe(false);
    }
  });
  it('does not upgrade non-human or contradictory payload authors through replay',()=>{
    for(const payload_author of [
      {actor_type:'agent',authorship:'generated',is_user:false},
      {actor_type:'agent',authorship:'original',is_user:true},
      {actor_type:'human',authorship:'generated',is_user:true},
      {actor_type:'unknown',authorship:'unknown',is_user:null}
    ]){
      const prepared=prepare([segment({provenance:{...human,payload_author}})],options);
      for(const chunk of prepared[0].chunks){
        expect(sourceHasHumanIntent(source(chunk))).toBe(false);
        expect(()=>resolveExtractionSources({type:'user_rule',scope:'global',source_refs:['S1']},[source(chunk)],{})).toThrow('human source');
      }
    }
  });
  it('keeps ordinary replay facts available without claiming that importer metadata proves human intent',()=>{
    const prepared=prepare([segment({})],options);
    const s=source(prepared[0].chunks[0]);
    expect(resolveExtractionSources({type:'system_fact',scope:'session',source_refs:['S1']},[s],{session_id:'session'})).toEqual([s]);
  });
  it('retains the original non-human envelope through export conversion',()=>{
    const prepared=prepare([segment({provenance:human,content:'[Inter-session message] sourceSession=sender isUser=false\nCancel reviewers.'})],options);
    for(const chunk of prepared[0].chunks)expect(sourceHasHumanIntent(source(chunk))).toBe(false);
  });
});
