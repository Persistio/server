import { z } from 'zod';

import type { MemoryScope } from './memory-scope';

export const MISSING_SCOPE_BINDING_POLICY_CODE = 'missing_scope_binding' as const;
export const FUTURE_SOURCE_TIMESTAMP_POLICY_CODE = 'future_source_timestamp' as const;
export const MAX_SOURCE_CLOCK_SKEW_MS = 5 * 60 * 1000;

export function isFutureSourceTimestamp(timestamp: string, now = new Date()): boolean {
  return new Date(timestamp).getTime() > now.getTime() + MAX_SOURCE_CLOCK_SKEW_MS;
}

export const contextIdentitySchema = z.string().max(512).refine(
  (value) => !/\p{Cc}/u.test(value),
  'Context identities must not contain control characters'
).transform((value) => value.trim()).pipe(z.string().min(1));

export const recallContextSchema = z.object({
  session_id: contextIdentitySchema.optional(),
  project_id: contextIdentitySchema.optional(),
  task_id: contextIdentitySchema.optional(),
  agent_id: contextIdentitySchema.optional(),
  trigger_type: z.enum(['direct', 'delegated', 'scheduled', 'event', 'backfill', 'api', 'unknown']).optional()
}).strict();

export type RecallContext = z.infer<typeof recallContextSchema>;

export function scopeKeyForContext(scope: MemoryScope, context: RecallContext): string | null {
  let key: string | undefined;
  switch (scope) {
    case 'project': key = context.project_id; break;
    case 'task': key = context.task_id; break;
    case 'session': key = context.session_id; break;
    default: return null;
  }
  const parsed = contextIdentitySchema.safeParse(key);
  // API inputs have already been normalized. Persisted legacy identities must
  // not be silently rebound to another identity while deriving new memory.
  return parsed.success && parsed.data === key ? parsed.data : null;
}

export function canIncludeGlobalRules(
  requested: boolean,
  mode: 'agent' | 'factual',
  context: RecallContext
): boolean {
  return requested
    && mode === 'agent'
    && Boolean(context.agent_id)
    && (context.trigger_type === 'direct'
      || context.trigger_type === 'delegated'
      || context.trigger_type === 'event'
      || context.trigger_type === 'api');
}

export interface ApplicableMemory {
  type: string | null;
  scope: MemoryScope;
  scope_key: string | null;
  sensitivity: string;
  confidence: number;
  source_timestamp: string | null;
}

export function isMemoryApplicable(
  memory: ApplicableMemory,
  context: RecallContext,
  includeGlobalRules: boolean,
  now = new Date()
): boolean {
  if (memory.sensitivity === 'restricted' || !(memory.confidence > 0 && memory.confidence <= 1)) {
    return false;
  }
  if (memory.source_timestamp) {
    const sourceTimestamp = new Date(memory.source_timestamp).getTime();
    if (!Number.isFinite(sourceTimestamp) || isFutureSourceTimestamp(memory.source_timestamp, now)) {
      return false;
    }
  }
  if (memory.scope === 'global') {
    return memory.scope_key == null && (memory.type !== 'user_rule' || includeGlobalRules);
  }
  const expectedKey = scopeKeyForContext(memory.scope, context);
  return expectedKey !== null && memory.scope_key === expectedKey;
}

export function memoryApplicabilityPredicateSql(
  memoryAlias: string,
  sessionParameter: string,
  projectParameter: string,
  taskParameter: string,
  includeGlobalRulesParameter: string
): string {
  return `(
    (${memoryAlias}.scope = 'global' AND ${memoryAlias}.scope_key IS NULL AND (
      ${memoryAlias}.type IS DISTINCT FROM 'user_rule'
      OR ${includeGlobalRulesParameter}::boolean
    ))
    OR (${memoryAlias}.scope = 'project' AND ${memoryAlias}.scope_key IS NOT NULL AND ${memoryAlias}.scope_key = ${projectParameter}::text)
    OR (${memoryAlias}.scope = 'task' AND ${memoryAlias}.scope_key IS NOT NULL AND ${memoryAlias}.scope_key = ${taskParameter}::text)
    OR (${memoryAlias}.scope = 'session' AND ${memoryAlias}.scope_key IS NOT NULL AND ${memoryAlias}.scope_key = ${sessionParameter}::text)
  )`;
}

export function memoryEligibilityPredicateSql(memoryAlias: string, recallTimeParameter: string): string {
  return `${memoryAlias}.sensitivity <> 'restricted'
    AND ${memoryAlias}.confidence > 0
    AND ${memoryAlias}.confidence <= 1
    AND (${memoryAlias}.source_timestamp IS NULL OR ${memoryAlias}.source_timestamp <= ${recallTimeParameter}::timestamptz + interval '5 minutes')`;
}
