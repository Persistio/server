import { contextIdentitySchema } from './memory-applicability';

export const MEMORY_SCOPES = ['global', 'project', 'task', 'session'] as const;

export type MemoryScope = typeof MEMORY_SCOPES[number];

/** PATCH must never transfer an omitted binding between identity namespaces. */
export function resolveMemoryScopeChange(
  current: {scope: MemoryScope; scope_key: string | null},
  patch: {scope?: MemoryScope; scope_key?: string | null; scope_change_reason?: string}
): {scope: MemoryScope; scopeKey: string | null; changedScope: boolean} {
  const invalid = (message: string): never => {throw Object.assign(new Error(message), {statusCode: 400});};
  const scope = patch.scope ?? current.scope;
  if (!parseMemoryScope(scope)) return invalid('Invalid memory scope');
  if (scope !== current.scope && scope !== 'global' && patch.scope_key == null) {
    return invalid('A changed scope requires an explicit binding');
  }
  if (scope === 'global' && patch.scope_key != null) return invalid('Global memory cannot have a scope binding');
  const scopeKey = scope === 'global' ? null : patch.scope_key !== undefined ? patch.scope_key : current.scope_key;
  if (scope !== 'global') {
    const parsed = contextIdentitySchema.safeParse(scopeKey);
    if (!parsed.success || parsed.data !== scopeKey) return invalid('Memory scope needs a valid binding');
  }
  const changedScope = scope !== current.scope || scopeKey !== current.scope_key;
  if (changedScope && !patch.scope_change_reason?.trim()) return invalid('Scope change reason is required');
  return {scope, scopeKey, changedScope};
}

export const INVALID_SCOPE_POLICY_CODE = 'invalid_memory_scope' as const;

const SCOPE_BREADTH: Record<MemoryScope, number> = {
  session: 0,
  task: 1,
  project: 2,
  global: 3
};

export function parseMemoryScope(value: unknown): MemoryScope | null {
  return typeof value === 'string' && MEMORY_SCOPES.includes(value as MemoryScope)
    ? value as MemoryScope
    : null;
}

export function isScopeWidening(current: MemoryScope, requested: MemoryScope): boolean {
  return SCOPE_BREADTH[requested] > SCOPE_BREADTH[current];
}

/**
 * Automatic/model-driven merges may preserve or narrow scope, but never widen it.
 * This breadth ordering alone does not validate identity bindings; callers must
 * also enforce their explicit source/binding contract.
 */
export function leastPrivilegedScope(first: MemoryScope, second: MemoryScope): MemoryScope {
  return SCOPE_BREADTH[first] <= SCOPE_BREADTH[second] ? first : second;
}

export function isScopeNoBroaderThanSources(
  requested: MemoryScope,
  sources: readonly MemoryScope[]
): boolean {
  return sources.length > 0 && sources.every((source) => !isScopeWidening(source, requested));
}
