import { scopeKeyForContext, type RecallContext } from './memory-applicability';

/** Exact raw-context boundary, not the applicability scope of a resulting fact.
 * Absence is explicit, never a wildcard. Agent/trigger do not partition raw context.
 * Persisted noncanonical identities must not be normalized into another boundary.
 */
export function extractionSummaryBoundary(vaultId: string, context: RecallContext): {
  sessionId: string; boundary: string; cacheKey: string;
} | null {
  const sessionId = scopeKeyForContext('session', context);
  if (sessionId === null) return null;
  for (const scope of ['project', 'task'] as const) {
    if (context[`${scope}_id`] !== undefined && scopeKeyForContext(scope, context) === null) return null;
  }
  const project = context.project_id ?? null;
  const task = context.task_id ?? null;
  return {
    sessionId,
    boundary: JSON.stringify([project, task]),
    cacheKey: JSON.stringify([vaultId, sessionId, project, task])
  };
}
