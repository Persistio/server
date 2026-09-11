import { describe, expect, it } from 'vitest';

import {
  hashRecallQuery,
  isExactTerminalDeliveryRetry,
  isGlobalRuleDelivery,
  isUnapprovedDirectiveDelivery,
  InvalidDeliveryOutcomeError,
  validateRenderedDeliveryPartition
} from './memory-observability';

describe('memory delivery observability', () => {
  it('hashes queries deterministically without retaining query text', () => {
    expect(hashRecallQuery('sensitive query')).toMatch(/^[0-9a-f]{64}$/);
    expect(hashRecallQuery('sensitive query')).toBe(hashRecallQuery('sensitive query'));
    expect(hashRecallQuery('sensitive query')).not.toContain('sensitive');
  });

  it('requires rendered and dropped IDs to exactly partition selection', () => {
    const selected = new Set(['a', 'b', 'c']);
    expect(() => validateRenderedDeliveryPartition(selected, ['a'], ['b', 'c'])).not.toThrow();
    for (const [rendered, dropped] of [
      [['a'], ['b']],
      [['a', 'a'], ['b', 'c']],
      [['a'], ['a', 'b', 'c']],
      [['a'], ['b', 'unknown']]
    ] as Array<[string[], string[]]>) {
      expect(() => validateRenderedDeliveryPartition(selected, rendered, dropped))
        .toThrow(InvalidDeliveryOutcomeError);
    }
  });
});

describe('isExactTerminalDeliveryRetry', () => {
  const input = {
    vaultId: 'vault', deliveryId: 'delivery', renderedIds: ['one'],
    dropped: [{ id: 'two', reason: 'token_budget' }], tokenBudget: 100,
    renderedTokens: 75, truncated: true, renderTarget: 'prompt_context' as const
  };
  const existing = [
    { memory_id: 'one', stage: 'rendered' as const, drop_reason: null, token_budget: 100,
      rendered_tokens: 75, truncated: true, render_target: 'prompt_context' as const },
    { memory_id: 'two', stage: 'dropped' as const, drop_reason: 'token_budget', token_budget: 100,
      rendered_tokens: 75, truncated: true, render_target: 'prompt_context' as const }
  ];

  it('accepts only a byte-for-byte equivalent terminal outcome', () => {
    expect(isExactTerminalDeliveryRetry(existing, new Set(['one', 'two']), input)).toBe(true);
    expect(isExactTerminalDeliveryRetry(existing, new Set(['one', 'two']), {
      ...input, renderTarget: 'tool_response'
    })).toBe(false);
    expect(isExactTerminalDeliveryRetry(existing, new Set(['one', 'two']), {
      ...input, dropped: [{ id: 'two', reason: 'client_policy' }]
    })).toBe(false);
    expect(isExactTerminalDeliveryRetry(existing, new Set(['one', 'two']), {
      ...input, renderedTokens: 74
    })).toBe(false);
  });
});

describe('delivery security classification', () => {
  const base = {
    memoryId: 'memory', authorityVersion: 1, section: 'historical_facts',
    memoryType: 'system_fact', retrievalReason: 'semantic', scope: 'global',
    scopeBinding: null, authorityState: 'untrusted', authorityRequired: false,
    authorityApprovalValid: false, similarity: 0.8
  };

  it('counts global rules by stored type and scope rather than retrieval lane', () => {
    expect(isGlobalRuleDelivery({ ...base, memoryType: 'user_rule' })).toBe(true);
    expect(isGlobalRuleDelivery({ ...base, memoryType: 'user_rule', scope: 'project' })).toBe(false);
  });

  it('flags every unapproved directive class', () => {
    expect(isUnapprovedDirectiveDelivery({ ...base, memoryType: 'workflow' })).toBe(true);
    expect(isUnapprovedDirectiveDelivery({ ...base, memoryType: 'workflow', authorityApprovalValid: true })).toBe(false);
    expect(isUnapprovedDirectiveDelivery(base)).toBe(false);
  });
});
