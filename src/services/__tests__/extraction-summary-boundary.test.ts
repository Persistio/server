import { describe, expect, it } from 'vitest';
import { extractionSummaryBoundary } from '../extraction-summary-boundary';
import { recallContextSchema } from '../memory-applicability';

describe('exact extraction summary boundary identity', () => {
  it('requires a canonical session and never invents absent context', () => {
    expect(extractionSummaryBoundary('v', {})).toBeNull();
    for (const session_id of ['', ' ', ' padded ', 'bad\nkey', 'x'.repeat(513)]) {
      expect(extractionSummaryBoundary('v', { session_id })).toBeNull();
    }
    expect(extractionSummaryBoundary('v', { session_id: 's' })).toEqual({
      sessionId: 's', boundary: '[null,null]', cacheKey: '["v","s",null,null]'
    });
  });
  it('partitions vault/session/project/task including explicit absence, not trigger or agent', () => {
    const base = { session_id: 's' };
    const keys = [
      extractionSummaryBoundary('v', base), extractionSummaryBoundary('other', base),
      extractionSummaryBoundary('v', { session_id: 'other' }),
      ...[{ project_id: 'A' }, { project_id: 'B' }, { task_id: 'A' }, { task_id: 'B' },
        { project_id: 'A', task_id: 'A' }, { project_id: 'A', task_id: 'B' },
        { project_id: 'B', task_id: 'A' }].map(context => extractionSummaryBoundary('v', { ...base, ...context }))
    ].map(value => value!.cacheKey);
    expect(new Set(keys).size).toBe(keys.length);
    expect(extractionSummaryBoundary('v', { ...base, agent_id: 'other', trigger_type: 'backfill' }))
      .toEqual(extractionSummaryBoundary('v', base));
  });
  it('uses an unambiguous tuple, not delimiter concatenation or Unicode normalization', () => {
    expect(extractionSummaryBoundary('v:s', { session_id: 'a', project_id: 'b:c' })!.cacheKey)
      .not.toBe(extractionSummaryBoundary('v', { session_id: 's:a', project_id: 'b:c' })!.cacheKey);
    expect(extractionSummaryBoundary('v', { session_id: 's', project_id: 'é' })!.cacheKey)
      .not.toBe(extractionSummaryBoundary('v', { session_id: 's', project_id: 'e\u0301' })!.cacheKey);
    const identity = '😀'.repeat(512);
    expect(extractionSummaryBoundary('v', { session_id: identity, project_id: identity, task_id: identity })!.boundary)
      .toBe(JSON.stringify([identity, identity]));
  });
  it('uses normalized API identities but never rebinds invalid persisted project/task identities', () => {
    const normalized = recallContextSchema.parse({ session_id: ' s ', project_id: ' p ', task_id: ' t ' });
    expect(extractionSummaryBoundary('v', normalized)!.boundary).toBe('["p","t"]');
    for (const key of ['project_id', 'task_id'] as const) {
      for (const value of ['', ' ', ' padded ', 'bad\nkey', 'x'.repeat(513)]) {
        expect(extractionSummaryBoundary('v', { session_id: 's', [key]: value })).toBeNull();
      }
    }
  });
});
