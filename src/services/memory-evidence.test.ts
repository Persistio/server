import { describe, expect, it } from 'vitest';

import { mergeMemoryEvidence } from './memory-evidence';

describe('mergeMemoryEvidence', () => {
  it('preserves policy rejection metadata while replacing the human summary', () => {
    const evidence = mergeMemoryEvidence({
      summary: 'Original evidence.',
      policy_rejections: [{
        code: 'invalid_memory_scope',
        field: 'scope',
        reason: 'unsupported'
      }]
    }, 'Updated evidence.');

    expect(JSON.parse(String(evidence))).toEqual({
      summary: 'Updated evidence.',
      policy_rejections: [{
        code: 'invalid_memory_scope',
        field: 'scope',
        reason: 'unsupported'
      }]
    });
  });

  it('clears ordinary evidence without erasing quarantine metadata', () => {
    expect(mergeMemoryEvidence({ summary: 'Clear me.' }, null)).toBeNull();
    expect(JSON.parse(String(mergeMemoryEvidence({
      summary: 'Clear me.',
      policy_rejections: [{ code: 'policy', field: 'scope', reason: 'review' }]
    }, null)))).toEqual({
      summary: null,
      policy_rejections: [{ code: 'policy', field: 'scope', reason: 'review' }]
    });
  });

  it('deduplicates existing and incoming policy rejections', () => {
    const rejection = { code: 'policy', field: 'scope', reason: 'review' };
    const evidence = mergeMemoryEvidence(JSON.stringify({
      summary: null,
      policy_rejections: [rejection]
    }), null, [rejection]);

    expect(JSON.parse(String(evidence)).policy_rejections).toEqual([rejection]);
  });

  it('preserves legacy string evidence when only policy metadata is added', () => {
    const evidence = mergeMemoryEvidence('Legacy evidence.', undefined, [
      { code: 'policy', field: 'scope', reason: 'review' }
    ]);

    expect(JSON.parse(String(evidence))).toEqual({
      summary: 'Legacy evidence.',
      policy_rejections: [{ code: 'policy', field: 'scope', reason: 'review' }]
    });
  });
});
