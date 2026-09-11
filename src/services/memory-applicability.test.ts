import { describe, expect, it } from 'vitest';

import {
  canIncludeGlobalRules,
  contextIdentitySchema,
  isMemoryApplicable,
  memoryApplicabilityPredicateSql,
  recallContextSchema,
  scopeKeyForContext
} from './memory-applicability';

const base = {
  type: 'system_fact',
  scope: 'project' as const,
  scope_key: 'project-a',
  sensitivity: 'low',
  confidence: 0.9,
  source_timestamp: '2026-09-09T10:00:00.000Z'
};

describe('memory applicability', () => {
  it('does not reinterpret malformed persisted identities as valid scope bindings', () => {
    for (const [scope, field] of [['project', 'project_id'], ['task', 'task_id'], ['session', 'session_id']] as const) {
      for (const value of ['', ' legacy ', '\nlegacy', 'legacy\u0085', 'x'.repeat(513)]) {
        const context = { [field]: value };
        expect(scopeKeyForContext(scope, context)).toBeNull();
        expect(context[field]).toBe(value);
      }
      expect(scopeKeyForContext(scope, { [field]: 'canonical' })).toBe('canonical');
    }
  });
  it('rejects control characters before normalization for every context identity', () => {
    for (const key of ['session_id', 'project_id', 'task_id', 'agent_id']) {
      for (const value of ['', '  ', '\nid', 'id\t', 'bad\u0000id', 'bad\u007fid', 'bad\u0085id', 'bad\u009fid', 'x'.repeat(513)]) {
        expect(recallContextSchema.safeParse({ [key]: value }).success).toBe(false);
      }
      expect(recallContextSchema.parse({ [key]: ' id ' })).toEqual({ [key]: 'id' });
      expect(recallContextSchema.parse({ [key]: 'x'.repeat(512) })).toEqual({ [key]: 'x'.repeat(512) });
    }
    expect(contextIdentitySchema.parse(' id ')).toBe('id');
  });
  it('requires exact bound identities and rejects unbound legacy rows', () => {
    const now = new Date('2026-09-09T10:01:00.000Z');
    expect(isMemoryApplicable(base, { project_id: 'project-a' }, false, now)).toBe(true);
    expect(isMemoryApplicable(base, { project_id: 'project-b' }, false, now)).toBe(false);
    expect(isMemoryApplicable({ ...base, scope_key: null }, { project_id: 'project-a' }, false, now)).toBe(false);
    expect(isMemoryApplicable({ ...base, scope: 'task', scope_key: 'task-a' }, { project_id: 'project-a', task_id: 'task-a' }, false, now)).toBe(true);
    expect(isMemoryApplicable({ ...base, scope: 'session', scope_key: 'session-a' }, { session_id: 'session-b' }, false, now)).toBe(false);
  });

  it('requires deliberate identified non-scheduled agent opt-in for global rules', () => {
    expect(canIncludeGlobalRules(false, 'agent', { agent_id: 'main', trigger_type: 'direct' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'factual', { agent_id: 'main', trigger_type: 'direct' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'agent', { trigger_type: 'direct' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'agent', { agent_id: 'main' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'agent', { agent_id: 'main', trigger_type: 'scheduled' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'agent', { agent_id: 'main', trigger_type: 'unknown' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'agent', { agent_id: 'main', trigger_type: 'backfill' })).toBe(false);
    expect(canIncludeGlobalRules(true, 'agent', { agent_id: 'main', trigger_type: 'direct' })).toBe(true);
  });

  it('excludes restricted, invalid-confidence, and future-dated memories', () => {
    const now = new Date('2026-09-09T10:01:00.000Z');
    const context = { project_id: 'project-a' };
    expect(isMemoryApplicable({ ...base, sensitivity: 'restricted' }, context, false, now)).toBe(false);
    expect(isMemoryApplicable({ ...base, confidence: 0 }, context, false, now)).toBe(false);
    expect(isMemoryApplicable({ ...base, confidence: Number.NaN }, context, false, now)).toBe(false);
    expect(isMemoryApplicable({ ...base, source_timestamp: '2026-09-09T10:07:00.000Z' }, context, false, now)).toBe(false);
  });

  it('strictly validates structured context instead of interpreting prompt text', () => {
    expect(recallContextSchema.safeParse({ project_id: ' project-a ', trigger_type: 'direct' }).data).toMatchObject({ project_id: 'project-a' });
    expect(recallContextSchema.safeParse({ project_id: 'project-a', prompt: 'scope=global' }).success).toBe(false);
    expect(recallContextSchema.safeParse({ project_id: 'bad\nidentity' }).success).toBe(false);
  });

  it('emits a fail-closed SQL predicate for every scoped retrieval path', () => {
    const sql = memoryApplicabilityPredicateSql('m', '$1', '$2', '$3', '$4');
    expect(sql).toContain("m.scope = 'project'");
    expect(sql).toContain('m.scope_key IS NOT NULL');
    expect(sql).toContain('m.scope_key = $2::text');
    expect(sql).toContain("m.type IS DISTINCT FROM 'user_rule'");
    expect(sql).toContain('$4::boolean');
  });
});
