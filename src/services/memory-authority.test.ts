import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

import {
  isAuthorityRecallable,
  isBehavioralMemoryType,
  isGlobalRuleRecallable,
  isMemoryRecallable,
  memoryAuthorityPredicateSql
} from './memory-authority';

describe('memory authority policy', () => {
  it.each(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint'])(
    'classifies %s as behavioral',
    (type) => expect(isBehavioralMemoryType(type)).toBe(true)
  );

  it.each(['project', 'decision', 'system_fact', 'domain_knowledge', null])(
    'classifies %s as factual context',
    (type) => expect(isBehavioralMemoryType(type)).toBe(false)
  );

  it.each(['untrusted', 'proposed', 'revoked'])(
    'blocks active behavioral memory in %s authority state',
    (state) => expect(isAuthorityRecallable('user_rule', state, true)).toBe(false)
  );

  it('requires a matching approval event for approved behavioral memory', () => {
    expect(isAuthorityRecallable('user_rule', 'approved', false)).toBe(false);
    expect(isAuthorityRecallable('user_rule', 'approved', true)).toBe(true);
    expect(isAuthorityRecallable('system_fact', 'proposed', false)).toBe(true);
  });

  it('blocks misclassified directive text when its untrusted origin requires authority', () => {
    expect(isAuthorityRecallable('system_fact', 'proposed', false, true)).toBe(false);
    expect(isAuthorityRecallable('system_fact', 'approved', false, true)).toBe(false);
    expect(isAuthorityRecallable('system_fact', 'approved', true, true)).toBe(true);
  });

  it('retains the explicit authority behavior for nullable legacy types', () => {
    expect(isAuthorityRecallable(null, 'proposed', false, false)).toBe(true);
    expect(isAuthorityRecallable(null, 'proposed', false, true)).toBe(false);
    expect(isAuthorityRecallable(null, 'approved', true, true)).toBe(true);
  });

  it('implements the emergency global-rule policy', () => {
    expect(isGlobalRuleRecallable('off', 'approved', true)).toBe(false);
    expect(isGlobalRuleRecallable('approved_only', 'proposed', false)).toBe(false);
    expect(isGlobalRuleRecallable('approved_only', 'approved', false)).toBe(false);
    expect(isGlobalRuleRecallable('approved_only', 'approved', true)).toBe(true);
    expect(isGlobalRuleRecallable('approved_only', 'approved', true, true)).toBe(false);
    expect(isGlobalRuleRecallable('legacy', 'proposed', false)).toBe(false);
    expect(isGlobalRuleRecallable('legacy', 'proposed', false, false, true)).toBe(true);
    expect(isGlobalRuleRecallable('legacy', 'revoked', true)).toBe(false);
    expect(isGlobalRuleRecallable('legacy', 'proposed', false, true, true)).toBe(false);
    expect(isGlobalRuleRecallable('legacy', 'approved', true, false)).toBe(true);
    expect(isGlobalRuleRecallable('legacy', 'approved', false, false, true)).toBe(false);
  });

  it('keeps the durable authority projection identical to the shared runtime SQL predicate', () => {
    const sql = readFileSync(resolve(__dirname, '../db/migrations/053_contradiction_activation_schedule.sql'), 'utf8');
    const body = /CREATE(?: OR REPLACE)? FUNCTION contradiction_authority_eligible\([\s\S]+?AS \$\$\s*SELECT ([\s\S]+?);?\s*\$\$;/i.exec(sql)?.[1];
    expect(body, 'authority projection function must be present').toBeDefined();
    const normalized = (value: string) => value.replace(/\(memory_row\)\./g, 'memory_row.')
      .replace(/;\s*$/, '').replace(/\s+/g, ' ').trim();
    expect(normalized(body!)).toBe(normalized(memoryAuthorityPredicateSql('memory_row', 'global_policy')));
  });

  it('applies the global-rule kill switch through every recall lane', () => {
    expect(isMemoryRecallable('user_rule', 'global', 'approved', true, 'off')).toBe(false);
    expect(isMemoryRecallable('user_rule', 'project', 'approved', true, 'off')).toBe(true);
  });

  it('limits legacy recall to current migration provenance across every recall lane', () => {
    expect(isMemoryRecallable('user_rule', 'global', 'proposed', false, 'legacy', true, false, false)).toBe(false);
    expect(isMemoryRecallable('user_rule', 'global', 'proposed', false, 'legacy', true, false, true)).toBe(true);
  });

  it('keeps behavioral authority sticky after a type is relabelled as factual', () => {
    expect(isMemoryRecallable('system_fact', 'project', 'proposed', false, 'approved_only', true)).toBe(false);
    expect(isMemoryRecallable('system_fact', 'project', 'approved', true, 'approved_only', true)).toBe(true);
  });
});
