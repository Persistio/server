import { describe, expect, it } from 'vitest';

import {
  INVALID_SCOPE_QUARANTINE_SCOPE,
  isScopeNoBroaderThanSources,
  isScopeWidening,
  leastPrivilegedScope,
  parseMemoryScope
} from './memory-scope';

describe('memory scope policy', () => {
  it.each(['global', 'project', 'task', 'session'] as const)('accepts supported scope %s', (scope) => {
    expect(parseMemoryScope(scope)).toBe(scope);
  });

  it.each([undefined, null, '', 'GLOBAL', 'workspace', 'sessoin', 1])(
    'rejects missing or unsupported scope %j',
    (scope) => {
      expect(parseMemoryScope(scope)).toBeNull();
    }
  );

  it('uses session only as the explicit quarantine scope', () => {
    expect(INVALID_SCOPE_QUARANTINE_SCOPE).toBe('session');
  });

  it.each([
    ['session', 'task'],
    ['session', 'project'],
    ['session', 'global'],
    ['task', 'project'],
    ['task', 'global'],
    ['project', 'global']
  ] as const)('detects widening from %s to %s', (current, requested) => {
    expect(isScopeWidening(current, requested)).toBe(true);
    expect(isScopeWidening(requested, current)).toBe(false);
  });

  it.each(['global', 'project', 'task', 'session'] as const)(
    'does not classify unchanged %s scope as widening',
    (scope) => expect(isScopeWidening(scope, scope)).toBe(false)
  );

  it('classifies the complete scope transition matrix', () => {
    const ordered = ['session', 'task', 'project', 'global'] as const;
    for (const [currentIndex, current] of ordered.entries()) {
      for (const [requestedIndex, requested] of ordered.entries()) {
        expect(isScopeWidening(current, requested)).toBe(requestedIndex > currentIndex);
      }
    }
  });

  it('chooses the least privileged scope for automatic merges', () => {
    expect(leastPrivilegedScope('global', 'session')).toBe('session');
    expect(leastPrivilegedScope('project', 'task')).toBe('task');
    expect(leastPrivilegedScope('task', 'task')).toBe('task');
  });

  it('rejects a curator create broader than any source candidate', () => {
    expect(isScopeNoBroaderThanSources('global', ['global', 'session'])).toBe(false);
    expect(isScopeNoBroaderThanSources('session', ['global', 'session'])).toBe(true);
    expect(isScopeNoBroaderThanSources('session', [])).toBe(false);
  });
});
