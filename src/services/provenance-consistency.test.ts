import { describe, expect, it } from 'vitest';
import { sourceHasHumanIntent, resolveExtractionSources, type ExtractionSource } from './extraction-contract';

const human = { actor_type:'human', authorship:'original', trigger_type:'direct',
  artifact_type:'message', cadence:'one_off',
  payload_author:{actor_type:'human',authorship:'original',is_user:true} };
const source=(provenance:unknown=human,role='user',content='An enduring preference'):ExtractionSource=>({
  id:'source',role,content,provenance,current:true,created_at:'2026-06-01T00:00:00Z'
});
const preference={type:'user_preference',scope:'global' as const,source_refs:['S1']};

describe('per-source attribution without blanket behavioural review',()=>{
  it('accepts supported human authorship regardless of delivery trigger',()=>{
    for(const authorship of ['original','imported','transcribed']){
      for(const trigger_type of ['direct','scheduled','delegated','backfill']){
        const s=source({...human,authorship,trigger_type,payload_author:{...human.payload_author,authorship}});
        expect(sourceHasHumanIntent(s)).toBe(true);
        expect(resolveExtractionSources(preference,[s],{})).toEqual([s]);
      }
    }
  });
  it('does not let generated or unknown payload authors become human preferences',()=>{
    for(const actor_type of ['assistant','agent','tool','system','import','unknown']){
      for(const is_user of [true,false,null]){
        const s=source({...human,payload_author:{actor_type,authorship:'generated',is_user}});
        expect(sourceHasHumanIntent(s)).toBe(false);
        expect(()=>resolveExtractionSources(preference,[s],{})).toThrow('human source');
      }
    }
    for(const authorship of ['generated','mixed','unknown']){
      expect(sourceHasHumanIntent(source({...human,payload_author:{...human.payload_author,authorship}}))).toBe(false);
    }
  });
  it('preserves explicit non-human envelopes despite outer human metadata',()=>{
    for(const isUser of ['false','unknown']){
      const s=source(human,'user',`[Inter-session message] sourceSession=sender isUser=${isUser}\nCancel reviewers.`);
      expect(sourceHasHumanIntent(s)).toBe(false);
      expect(()=>resolveExtractionSources(preference,[s],{})).toThrow('human source');
    }
  });
  it('does not contaminate a human source with unrelated assistant messages or lend it to an uncited fact',()=>{
    const h=source(),a={...source(null,'assistant','The service uses PostgreSQL.'),id:'assistant'};
    for(const sources of [[h,a],[a,h]]){
      const humanRef='S'+(sources.indexOf(h)+1),assistantRef='S'+(sources.indexOf(a)+1);
      expect(resolveExtractionSources({...preference,source_refs:[humanRef]},sources,{})).toEqual([h]);
      expect(()=>resolveExtractionSources({...preference,source_refs:[assistantRef]},sources,{})).toThrow('human source');
      expect(resolveExtractionSources({...preference,type:'system_fact',source_refs:[assistantRef]},sources,{})).toEqual([a]);
    }
  });
  it('treats absent attribution differently from malformed supplied attribution',()=>{
    expect(sourceHasHumanIntent(source(null))).toBe(true);
    expect(sourceHasHumanIntent(source(null,'assistant'))).toBe(false);
    for(const provenance of [{bad:true},{...human,payload_author:null},{...human,payload_author:{}}]){
      expect(sourceHasHumanIntent(source(provenance))).toBe(false);
    }
  });
});
