export const MEMORY_SCOPES = ['global', 'project', 'task', 'session'] as const;

export type MemoryScope = typeof MEMORY_SCOPES[number];

export const INVALID_SCOPE_QUARANTINE_SCOPE: MemoryScope = 'session';
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
 * Until scopes have bound applicability IDs, this least-privilege ordering is the
 * only safe deterministic merge rule.
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
