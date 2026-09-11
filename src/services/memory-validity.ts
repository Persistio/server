const DATE_ONLY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export interface MemoryValidityWindow {
  valid_from: string | null;
  valid_until: string | null;
}

export const INVALID_VALIDITY_WINDOW_POLICY_CODE = 'invalid_memory_validity_window' as const;

export function toDateOnly(value: Date): string | null {
  const timestamp = value.getTime();
  return Number.isFinite(timestamp) ? value.toISOString().slice(0, 10) : null;
}

export function isValidDateOnly(value: string): boolean {
  // JavaScript accepts astronomical year zero, but PostgreSQL date does not.
  if (!DATE_ONLY_PATTERN.test(value) || value.startsWith('0000-')) {
    return false;
  }

  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

/**
 * Validity windows are inclusive. Null bounds are unbounded. Any malformed bound
 * or invalid reference time fails closed so an unexpected row representation can
 * never become prompt context through the application-layer defence.
 */
export function isMemoryValidAt(memory: MemoryValidityWindow, referenceTime: Date): boolean {
  const referenceDate = toDateOnly(referenceTime);
  if (!referenceDate) {
    return false;
  }

  if (memory.valid_from !== null) {
    if (!isValidDateOnly(memory.valid_from) || memory.valid_from > referenceDate) {
      return false;
    }
  }

  if (memory.valid_until !== null) {
    if (!isValidDateOnly(memory.valid_until) || memory.valid_until < referenceDate) {
      return false;
    }
  }

  return true;
}

export function memoryValidityPredicateSql(memoryAlias: string, referenceDateParameter: string): string {
  return `(
    (${memoryAlias}.valid_from IS NULL OR ${memoryAlias}.valid_from <= ${referenceDateParameter}::date)
    AND (${memoryAlias}.valid_until IS NULL OR ${memoryAlias}.valid_until >= ${referenceDateParameter}::date)
  )`;
}

export function validityWindowsOverlapPredicateSql(
  memoryAlias: string,
  incomingValidFromParameter: string,
  incomingValidUntilParameter: string
): string {
  return `(
    (${memoryAlias}.valid_until IS NULL OR ${incomingValidFromParameter}::date IS NULL OR ${memoryAlias}.valid_until >= ${incomingValidFromParameter}::date)
    AND (${incomingValidUntilParameter}::date IS NULL OR ${memoryAlias}.valid_from IS NULL OR ${incomingValidUntilParameter}::date >= ${memoryAlias}.valid_from)
  )`;
}

export function intersectValidityWindows(windows: MemoryValidityWindow[]): MemoryValidityWindow {
  let validFrom: string | null = null;
  let validUntil: string | null = null;

  for (const window of windows) {
    if (window.valid_from !== null) {
      if (!isValidDateOnly(window.valid_from)) {
        throw new Error(`Invalid validity-window start: ${window.valid_from}`);
      }
      if (validFrom === null) {
        validFrom = window.valid_from;
      } else if (window.valid_from > validFrom) {
        validFrom = window.valid_from;
      }
    }
    if (window.valid_until !== null) {
      if (!isValidDateOnly(window.valid_until)) {
        throw new Error(`Invalid validity-window end: ${window.valid_until}`);
      }
      if (validUntil === null) {
        validUntil = window.valid_until;
      } else if (window.valid_until < validUntil) {
        validUntil = window.valid_until;
      }
    }
  }

  if (validFrom !== null && validUntil !== null && validFrom > validUntil) {
    throw new Error(`Validity windows do not overlap: ${validFrom} > ${validUntil}`);
  }

  return { valid_from: validFrom, valid_until: validUntil };
}

/** Automatic consolidation may preserve or narrow a window, but never widen it. */
export function intersectValidityBoundSql(
  previousBoundSql: string,
  incomingBoundParameter: string,
  direction: 'lower' | 'upper'
): string {
  const comparator = direction === 'lower' ? 'GREATEST' : 'LEAST';
  return `CASE
    WHEN ${previousBoundSql} IS NULL THEN ${incomingBoundParameter}::date
    WHEN ${incomingBoundParameter}::date IS NULL THEN ${previousBoundSql}
    ELSE ${comparator}(${previousBoundSql}, ${incomingBoundParameter}::date)
  END`;
}
