import { isMemoryValidAt, isValidDateOnly, toDateOnly, type MemoryValidityWindow } from './memory-validity';

// Taxonomy is defined by prompts/extractor.txt, not ranking boosts or the narrower
// human-intent provenance gate (which covers only user_rule/user_preference).
export const BEHAVIORAL_MEMORY_TYPES = [
  'user_rule', 'user_preference', 'task_pattern', 'workflow', 'constraint', 'decision'
] as const;
export const FACTUAL_MEMORY_TYPES = ['project', 'system_fact', 'domain_knowledge'] as const;
export type MemoryTemporalState = 'historical' | 'future' | 'current_or_unbounded';
export interface TemporalRecallMemory extends MemoryValidityWindow { type: string | null }

export function hasWellFormedValidity(memory: MemoryValidityWindow): boolean {
  return (memory.valid_from === null || (typeof memory.valid_from === 'string' && isValidDateOnly(memory.valid_from)))
    && (memory.valid_until === null || (typeof memory.valid_until === 'string' && isValidDateOnly(memory.valid_until)))
    && (memory.valid_from === null || memory.valid_until === null || memory.valid_from <= memory.valid_until);
}

/** Null means unusable metadata, never an implicit current/unbounded fallback. */
export function memoryTemporalState(memory: MemoryValidityWindow, now: Date): MemoryTemporalState | null {
  const date = toDateOnly(now);
  if (!date || !isValidDateOnly(date) || !hasWellFormedValidity(memory)) return null;
  return memory.valid_until !== null && memory.valid_until < date ? 'historical'
    : memory.valid_from !== null && memory.valid_from > date ? 'future' : 'current_or_unbounded';
}

/** Behavioral applicability is an eligibility boundary, not just a bundle label. */
export function isMemoryTemporallyRecallable(memory: TemporalRecallMemory, now: Date): boolean {
  if (memoryTemporalState(memory, now) === null) return false;
  if ((FACTUAL_MEMORY_TYPES as readonly unknown[]).includes(memory.type)) return true;
  return (BEHAVIORAL_MEMORY_TYPES as readonly unknown[]).includes(memory.type) && isMemoryValidAt(memory, now);
}

/** Only trusted SQL identifiers/parameter expressions belong here; no user text.
 * PostgreSQL date columns already reject malformed calendars/year zero. Range
 * guards additionally reject infinities, BC and >9999 years, matching TypeScript.
 * The supplied request timestamp, not the DB clock/session timezone, owns the date.
 */
export function memoryTemporalRecallPredicateSql(alias: string, referenceTime: string): string {
  const date = `((${referenceTime})::timestamptz AT TIME ZONE 'UTC')::date`;
  const literals = (types: readonly string[]) => types.map(type => `'${type}'`).join(',');
  return `(
    ${date} BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'
    AND (${alias}.valid_from IS NULL OR ${alias}.valid_from BETWEEN DATE '0001-01-01' AND DATE '9999-12-31')
    AND (${alias}.valid_until IS NULL OR ${alias}.valid_until BETWEEN DATE '0001-01-01' AND DATE '9999-12-31')
    AND (${alias}.valid_from IS NULL OR ${alias}.valid_until IS NULL OR ${alias}.valid_from <= ${alias}.valid_until)
    AND (${alias}.type IN (${literals(FACTUAL_MEMORY_TYPES)})
      OR (${alias}.type IN (${literals(BEHAVIORAL_MEMORY_TYPES)})
        AND (${alias}.valid_from IS NULL OR ${alias}.valid_from <= ${date})
        AND (${alias}.valid_until IS NULL OR ${alias}.valid_until >= ${date})))
  )`;
}
