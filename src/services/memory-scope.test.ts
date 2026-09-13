import { describe, expect, it } from 'vitest';

import {
  isScopeNoBroaderThanSources,
  isScopeWidening,
  leastPrivilegedScope,
  parseMemoryScope,
  resolveMemoryScopeChange,
  MEMORY_SCOPES
} from './memory-scope';

describe('memory scope policy', () => {
  it('validates the entire PATCH namespace/key matrix, including identical key text across namespaces',()=>{
    for(const scope of MEMORY_SCOPES)for(const target of MEMORY_SCOPES){
      const current={scope,scope_key:scope==='global'?null:'binding'};
      for(const key of [{},{scope_key:null},{scope_key:'binding'},{scope_key:'new-binding'}]){
        const patch={scope:target,...key,scope_change_reason:'Explicit user change'};
        const valid=target==='global'?key.scope_key==null
          : typeof key.scope_key==='string'||(!('scope_key' in key)&&target===scope);
        if(!valid){expect(()=>resolveMemoryScopeChange(current,patch)).toThrow();continue;}
        const result=resolveMemoryScopeChange(current,patch);
        expect(result.scope).toBe(target);
        expect(result.scopeKey).toBe(target==='global'?null:key.scope_key??current.scope_key);
        expect(result.changedScope).toBe(scope!==target||current.scope_key!==result.scopeKey);
        const noReason={...patch,scope_change_reason:undefined};
        if(result.changedScope)expect(()=>resolveMemoryScopeChange(current,noReason)).toThrow('reason');
        else expect(resolveMemoryScopeChange(current,noReason)).toEqual(result);
      }
    }
  });
  it.each(['',' ',' key','key ','a\nb','x'.repeat(513)])('rejects invalid stored/effective identity %j',scope_key=>{
    expect(()=>resolveMemoryScopeChange({scope:'session',scope_key},{})).toThrow();
  });
  it('preserves the binding for an ordinary edit but requires a reason for a same-namespace rebind',()=>{
    const current={scope:'task' as const,scope_key:'t1'};
    expect(resolveMemoryScopeChange(current,{})).toEqual({scope:'task',scopeKey:'t1',changedScope:false});
    expect(()=>resolveMemoryScopeChange(current,{scope_key:'t2'})).toThrow('reason');
  });
  it.each(['global', 'project', 'task', 'session'] as const)('accepts supported scope %s', (scope) => {
    expect(parseMemoryScope(scope)).toBe(scope);
  });

  it.each([undefined, null, '', 'GLOBAL', 'workspace', 'sessoin', 1])(
    'rejects missing or unsupported scope %j',
    (scope) => {
      expect(parseMemoryScope(scope)).toBeNull();
    }
  );

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
