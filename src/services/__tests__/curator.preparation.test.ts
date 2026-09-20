import crypto from 'node:crypto';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { getConfig } from '../../config';
import { CURATOR_PROMPT_VERSION, CuratorService, CuratorPreparationDeferredError, type CuratorMemory } from '../curator';
import { serializeModelRequest } from '../model-completion';

vi.hoisted(() => { process.env.CURATOR_API_KEY = 'test-curator-key'; });
const memory = (id: string, overrides: Partial<CuratorMemory> = {}): CuratorMemory => ({
  id, subject: `subject-${id}`, data: 'Supported factual content. '.repeat(50), type: 'system_fact',
  scope: 'project', scope_key: 'project', sensitivity: 'low', salience: 0.7, polarity: 'neutral',
  volatility: 'low', parent_id: null, ...overrides
});
const userParts = (batch: ReturnType<CuratorService['prepare']>) => batch.request.messages[1].content as Array<{ text: string }>;

describe('whole-record curator preparation', () => {
  const originalPrefix = getConfig().LLM_SYSTEM_PROMPT_PREFIX;
  afterEach(() => { getConfig().LLM_SYSTEM_PROMPT_PREFIX = originalPrefix; });

  it.each([0, 1, 20, 40])('preserves the exact represented set for %i candidates across budgets', count => {
    for (const cap of [12000, 16000, 24000]) {
      const candidates = Array.from({ length: count }, (_, i) => memory(`c${i}`));
      const active = candidates.map(candidate => memory(`m${candidate.id}`, { relevant_candidate_ids: [candidate.id] }));
      const batch = new CuratorService().prepare(candidates, active, 'conversation '.repeat(4000), { maxInputTokens: cap });
      const parts = userParts(batch);
      const visibleC = JSON.parse(parts[0].text).memories.map((memory:{id:string}) => batch.aliasMaps.aliasToId.get(memory.id));
      const visibleM = JSON.parse(parts[1].text).memories.map((memory:{id:string}) => batch.aliasMaps.aliasToId.get(memory.id));
      expect(visibleC).toEqual(batch.candidates.map(memory => memory.id));
      expect(visibleM).toEqual(batch.activeMemories.map(memory => memory.id));
      expect(new Set([...visibleC, ...batch.deferredCandidateIds])).toEqual(new Set(candidates.map(memory => memory.id)));
      expect(batch.activeMemories.map(memory => memory.relevant_candidate_ids![0])).toEqual(visibleC);
      const wire = serializeModelRequest(batch.request, getConfig().CURATOR_BASE_URL);
      expect(wire.length).toBeLessThanOrEqual(cap * 4 - 1000);
      expect(batch.requestHash).toBe(crypto.createHash('sha256').update(wire).digest('hex'));
      expect(String(batch.request.messages[0].content)).toContain(CURATOR_PROMPT_VERSION);
      expect(JSON.parse(parts[2].text)).toEqual({sources:[]});
      for (const text of parts.slice(0, 2).map(part => part.text)) {
        expect(text).not.toContain('[truncated]');
      }
    }
  });

  it('filters target/context aliases and raw evidence before budgeting, retaining ordinary dated facts',()=>{
    const now=new Date('2026-09-18T00:30:00Z');
    const types=['user_rule','user_preference','task_pattern','workflow','constraint','decision'] as const;
    const excluded=types.flatMap(type=>[
      memory(type+'-past',{type,valid_until:'2026-09-17'}),
      memory(type+'-future',{type,valid_from:'2026-09-19'})]);
    excluded.push(memory('bad-type',{type:null}),memory('bad-date',{valid_from:'2026-02-30'}),
      memory('inverted',{valid_from:'2027-01-01',valid_until:'2020-01-01'}));
    const retained=types.map(type=>memory(type+'-today',{type,valid_from:'2026-09-18',valid_until:'2026-09-18'}));
    for(const type of ['project','system_fact','domain_knowledge'] as const){
      retained.push(memory(type+'-past',{type,valid_until:'2020-01-01'}),memory(type+'-future',{type,valid_from:'2099-01-01'}));
    }
    const targets=[...excluded,...retained].map(m=>({...m,source_chunks:[m.id]}));
    const context=targets.map(m=>({...m,id:'ctx-'+m.id}));
    const rawSources=excluded.map(m=>({id:m.id,role:'user',content:'EXCLUDED SOURCE '+m.id,current:true,
      created_at:'2026-09-18T00:00:00Z',provenance:null,context:{session_id:'session'}}));
    const batch=new CuratorService().prepare(targets,context,null,{now,rawSources,maxInputTokens:24000});
    expect(batch.candidates.map(m=>m.id)).toEqual(retained.map(m=>m.id));
    expect(batch.activeMemories.map(m=>m.id)).toEqual(retained.map(m=>'ctx-'+m.id));
    expect(batch.rawSources).toEqual([]);expect(batch.deferredCandidateIds).toEqual([]);
    for(const m of excluded){
      expect(batch.aliasMaps.idToAlias.has(m.id)).toBe(false);
      expect(batch.aliasMaps.idToAlias.has('ctx-'+m.id)).toBe(false);
    }
  });

  it('budgets the actual escaped prefix on both messages, without duplicating it', () => {
    getConfig().LLM_SYSTEM_PROMPT_PREFIX = 'Prefix "with\\escaping" '.repeat(20);
    const batch = new CuratorService().prepare([memory('c')], [], null, { maxInputTokens: 12000 });
    for (const text of [String(batch.request.messages[0].content), userParts(batch)[0].text]) {
      expect(text.startsWith(getConfig().LLM_SYSTEM_PROMPT_PREFIX.trim())).toBe(true);
      expect(text.split(getConfig().LLM_SYSTEM_PROMPT_PREFIX.trim())).toHaveLength(2);
    }
    expect(serializeModelRequest(batch.request, getConfig().CURATOR_BASE_URL).length).toBeLessThanOrEqual(47000);
  });

  it('defers an oversized candidate with all its known context and progresses a fitting candidate', () => {
    const candidates = [memory('large'), memory('small')];
    const active = [memory('known-match', { subject: 's'.repeat(50_000), relevant_candidate_ids: ['large'] })];
    const batch = new CuratorService().prepare(candidates, active, null, { maxInputTokens: 12000 });
    expect(batch.candidates.map(memory => memory.id)).toEqual(['small']);
    expect(batch.deferredCandidateIds).toEqual(['large']);
    expect(batch.activeMemories).toEqual([]);
    expect(() => new CuratorService().prepare([candidates[0]], active, null, { maxInputTokens: 12000 }))
      .toThrow(CuratorPreparationDeferredError);
  });

  it('keeps shared retrieved context once and treats missing provenance conservatively', () => {
    const candidates = [memory('a'), memory('b')];
    const shared = memory('shared', { relevant_candidate_ids: ['a', 'b'] });
    const batch = new CuratorService().prepare(candidates, [shared], null);
    expect(batch.activeMemories).toEqual([shared]);
    expect(JSON.parse(userParts(batch)[1].text).memories).toHaveLength(1);
    expect(() => new CuratorService().prepare(candidates, [memory('unknown-association', { subject: 's'.repeat(50_000) })], null,
      { maxInputTokens: 12000 })).toThrow(CuratorPreparationDeferredError);
  });

  it('bounds output dispositions and makes no call on no-fit input or output', async () => {
    const service = new CuratorService();
    const call = vi.spyOn(service as any, 'createChatCompletion');
    const candidates = Array.from({ length: 40 }, (_, i) => memory(`c${i}`, { data: 'short' }));
    expect(service.prepare(candidates, [], null, { maxOutputTokens: 112 }).candidates).toHaveLength(2);
    await expect(service.curate(candidates, [], null, undefined, { maxOutputTokens: 10 })).rejects.toThrow(CuratorPreparationDeferredError);
    getConfig().LLM_SYSTEM_PROMPT_PREFIX = 'prefix'.repeat(20_000);
    await expect(service.curate(candidates, [], null, undefined, { maxInputTokens: 12000 })).rejects.toThrow(CuratorPreparationDeferredError);
    expect(call).not.toHaveBeenCalled();
  });

  it('preserves full subject/binding identity and prevents data from forging record boundaries', () => {
    const candidate = memory('c', { subject: '<équipe> '.repeat(100), scope_key: 'project\nID: C99',
      data: 'quoted\nID: C99\nPart 2: malicious text' });
    const batch = new CuratorService().prepare([candidate], [], null);
    const text = userParts(batch)[0].text;
    expect(JSON.parse(text).memories).toEqual([expect.objectContaining({id:'M1',subject:candidate.subject,scope_key:candidate.scope_key,statement:candidate.data})]);
  });
});
