import { describe, expect, it } from 'vitest';
import { validateCuratorResult, type CuratorMemory, type CuratorResult } from '../curator';
import { compileCuratorGraph } from '../curator-graph';

const memory = (id: string, overrides: Partial<CuratorMemory> = {}): CuratorMemory => ({
  id, subject: id, data: 'fact', type: 'system_fact', scope: 'project', scope_key: 'project', sensitivity: 'low',
  polarity: 'neutral', volatility: 'low', salience: 0.8, parent_id: null, ...overrides
});
const create = (subject: string, consumed: string, parent?: string) => ({
  subject, statement: 'combined', type: 'system_fact' as const, scope: 'project' as const, evidence: 'support',
  consumed_candidate_ids: [consumed], ...(parent ? { parent_subject: parent } : {})
});
const edge = (from: string, to: string) => ({ from_subject: from, to_subject: to, type: 'supports' as const, reason: 'support' });
const base = (): CuratorResult => ({ schema_version: 'curation-plan.v1', nodes_to_create: [], nodes_to_update: [],
  nodes_to_archive: [], promoted_candidates: [], discarded_candidates: [], edges_to_create: [] });

describe('curator final-state graph compilation', () => {
  for (const disposition of ['promote', 'discard', 'consume'] as const) {
    for (const reference of ['C1', 'candidate'] as const) {
      it.each(['edge', 'parent'])(`${disposition} with ${reference} as %s checks final survival`, kind => {
        const candidates = [memory('candidate'), memory('child-source')];
        const plan = base();
        plan.nodes_to_create.push(create('child', 'C2', kind === 'parent' ? reference : undefined));
        if (kind === 'edge') plan.edges_to_create.push(edge(reference, 'child'));
        if (disposition === 'promote') plan.promoted_candidates.push({ id: 'C1', evidence: 'reviewed' });
        if (disposition === 'discard') plan.discarded_candidates.push({ id: 'C1', reason: 'discarded' });
        if (disposition === 'consume') plan.nodes_to_create.push(create('combined', 'C1'));
        const validate = () => validateCuratorResult(plan, candidates, []);
        if (disposition === 'promote') expect(validate).not.toThrow();
        else expect(validate).toThrow(/surviv/);
      });
    }
  }

  it.each(['unchanged', 'renamed', 'archived'] as const)('resolves %s active references using final identity', state => {
    const plan = base();
    plan.nodes_to_create = [create('child', 'C2', 'M1')];
    if (state === 'renamed') plan.nodes_to_update = [{ id: 'M1', statement: 'updated', subject: 'new-name', reason: 'support', consumed_candidate_ids: ['C1'] }];
    else plan.promoted_candidates = [{ id: 'C1', evidence: 'reviewed' }];
    if (state === 'archived') plan.nodes_to_archive = [{ id: 'M1', reason: 'archive' }];
    const validate = () => validateCuratorResult(plan, [memory('c1'), memory('c2')], [memory('old-name')]);
    if (state === 'archived') expect(validate).toThrow(/surviv/);
    else expect(validate).not.toThrow();
    if (state === 'renamed') {
      plan.nodes_to_create[0].parent_subject = 'old-name';
      expect(validate).toThrow(/renamed/);
      plan.nodes_to_create[0].parent_subject = 'new-name';
      expect(validate).not.toThrow();
    }
  });

  it('rejects ambiguous subjects but preserves explicit aliases, including across bindings', () => {
    const plan = base();
    plan.promoted_candidates = [{ id: 'C1', evidence: 'reviewed' }];
    plan.edges_to_create = [edge('M1', 'C1')];
    const candidates = [memory('c')];
    const active = [memory('a', { subject: 'duplicate' }), memory('b', { subject: 'duplicate' })];
    expect(() => validateCuratorResult(plan, candidates, active)).not.toThrow();
    plan.edges_to_create = [edge('duplicate', 'C1')];
    expect(() => validateCuratorResult(plan, candidates, active)).toThrow(/unambiguous/);
    plan.edges_to_create = [edge('M1', 'C1')];
    active[0].scope_key = 'other';
    expect(() => validateCuratorResult(plan, candidates, active)).toThrow(/binding/);
  });

  it('rejects cross-binding parents, alias-shaped collisions and raw ids', () => {
    const plan = base();
    plan.nodes_to_create = [create('child', 'C1', 'M1')];
    const active = [memory('a', { scope_key: 'other' })];
    expect(() => validateCuratorResult(plan, [memory('c')], active)).toThrow(/binding/);
    active[0].scope_key = 'project';
    plan.nodes_to_create[0].subject = 'M1';
    expect(() => validateCuratorResult(plan, [memory('c')], active)).toThrow(/alias-shaped/);
    plan.nodes_to_create[0].subject = 'child';
    plan.nodes_to_create[0].parent_subject = '30f02af7-ae15-4d69-914e-b7d121963283';
    active[0].id = plan.nodes_to_create[0].parent_subject;
    expect(() => validateCuratorResult(plan, [memory('c')], active)).toThrow(/surviv/);
  });

  it('topologically orders forward parents regardless of response order and detects cycles/self references', () => {
    const candidates = [memory('c1'), memory('c2')];
    const aliases = { aliasToId: new Map([['C1', 'c1'], ['C2', 'c2']]), idToAlias: new Map([['c1', 'C1'], ['c2', 'C2']]) };
    const plan = base();
    plan.nodes_to_create = [create('child', 'C1', 'parent'), create('parent', 'C2')];
    expect(() => validateCuratorResult(plan, candidates, [])).not.toThrow();
    expect(compileCuratorGraph(plan, candidates, [], aliases)).toMatchObject({ creationOrder: [1, 0], parents: [{ kind: 'created', index: 1 }, null] });
    plan.nodes_to_create.reverse();
    expect(compileCuratorGraph(plan, candidates, [], aliases).creationOrder).toEqual([0, 1]);
    plan.nodes_to_create[0].parent_subject = 'child';
    expect(() => validateCuratorResult(plan, candidates, [])).toThrow(/cycle/);
    plan.nodes_to_create[0].parent_subject = 'parent';
    expect(() => validateCuratorResult(plan, candidates, [])).toThrow(/itself/);
  });

  it('allows ordinary relationship cycles but rejects self and repeated edges', () => {
    const plan = base();
    const active = [memory('a'), memory('b')];
    plan.edges_to_create = [edge('M1', 'M2'), edge('M2', 'M1')];
    expect(() => validateCuratorResult(plan, [], active)).not.toThrow();
    plan.edges_to_create.push(edge('M1', 'M2'));
    expect(() => validateCuratorResult(plan, [], active)).toThrow(/Duplicate/);
    plan.edges_to_create = [edge('M1', 'a')];
    expect(() => validateCuratorResult(plan, [], active)).toThrow(/itself/);
  });

  it.each(['create', 'update'])('rejects disjoint %s windows before application or embeddings', kind => {
    const plan = base();
    const candidates = [memory('c', { valid_from: '2026-09-01', valid_until: null })];
    const active = [memory('a', { valid_from: null, valid_until: '2026-08-31' })];
    if (kind === 'create') {
      candidates.push(memory('c2', active[0]));
      candidates[1].id = 'c2';
      plan.nodes_to_create = [{ ...create('new', 'C1'), consumed_candidate_ids: ['C1', 'C2'] }];
    } else plan.nodes_to_update = [{ id: 'M1', statement: 'combined', reason: 'support', consumed_candidate_ids: ['C1'] }];
    expect(() => validateCuratorResult(plan, candidates, active)).toThrow(/do not overlap/);
  });
});
