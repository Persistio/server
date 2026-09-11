import { describe, expect, it } from 'vitest';

import {
  intersectValidityBoundSql,
  intersectValidityWindows,
  isMemoryValidAt,
  isValidDateOnly,
  memoryValidityPredicateSql,
  toDateOnly,
  validityWindowsOverlapPredicateSql
} from './memory-validity';

describe('memory validity', () => {
  const now = new Date('2026-05-30T23:59:59.999Z');

  it('treats null bounds as unbounded and both date boundaries as inclusive', () => {
    expect(isMemoryValidAt({ valid_from: null, valid_until: null }, now)).toBe(true);
    expect(isMemoryValidAt({ valid_from: '2026-05-30', valid_until: '2026-05-30' }, now)).toBe(true);
    expect(isMemoryValidAt({ valid_from: '2026-05-01', valid_until: '2026-06-01' }, now)).toBe(true);
  });

  it('rejects future, expired, inverted, and malformed windows', () => {
    expect(isMemoryValidAt({ valid_from: '2026-05-31', valid_until: null }, now)).toBe(false);
    expect(isMemoryValidAt({ valid_from: null, valid_until: '2026-05-29' }, now)).toBe(false);
    expect(isMemoryValidAt({ valid_from: '2026-06-01', valid_until: '2026-05-01' }, now)).toBe(false);
    expect(isMemoryValidAt({ valid_from: '2026-02-30', valid_until: null }, now)).toBe(false);
    expect(isMemoryValidAt({ valid_from: null, valid_until: 'not-a-date' }, now)).toBe(false);
    expect(isMemoryValidAt({ valid_from: null, valid_until: null }, new Date('invalid'))).toBe(false);
  });

  it('validates and derives UTC date-only values deterministically', () => {
    expect(isValidDateOnly('2024-02-29')).toBe(true);
    expect(isValidDateOnly('2025-02-29')).toBe(false);
    expect(isValidDateOnly('2026-5-01')).toBe(false);
    expect(isValidDateOnly('0000-01-01')).toBe(false);
    expect(isValidDateOnly('0001-01-01')).toBe(true);
    expect(isValidDateOnly('9999-12-31')).toBe(true);
    expect(toDateOnly(now)).toBe('2026-05-30');
  });

  it('builds an inclusive SQL predicate against a caller-supplied date parameter', () => {
    const predicate = memoryValidityPredicateSql('memory', '$7');

    expect(predicate).toContain('memory.valid_from IS NULL OR memory.valid_from <= $7::date');
    expect(predicate).toContain('memory.valid_until IS NULL OR memory.valid_until >= $7::date');
    expect(predicate).not.toContain('CURRENT_DATE');
  });

  it('builds least-authority intersections for automatic validity-window merges', () => {
    const lower = intersectValidityBoundSql('target.previous_valid_from', '$11', 'lower');
    const upper = intersectValidityBoundSql('target.previous_valid_until', '$12', 'upper');

    expect(lower).toContain('GREATEST(target.previous_valid_from, $11::date)');
    expect(upper).toContain('LEAST(target.previous_valid_until, $12::date)');
    expect(lower).not.toContain('LEAST(');
    expect(upper).not.toContain('GREATEST(');
  });

  it('builds an overlap guard for automatic match selection', () => {
    const predicate = validityWindowsOverlapPredicateSql('memory', '$8', '$9');

    expect(predicate).toContain('memory.valid_until IS NULL OR $8::date IS NULL OR memory.valid_until >= $8::date');
    expect(predicate).toContain('$9::date IS NULL OR memory.valid_from IS NULL OR $9::date >= memory.valid_from');
  });

  it('intersects every source window for curator-created memories', () => {
    expect(intersectValidityWindows([
      { valid_from: null, valid_until: '2026-06-30' },
      { valid_from: '2026-05-01', valid_until: null },
      { valid_from: '2026-05-15', valid_until: '2026-06-15' }
    ])).toEqual({ valid_from: '2026-05-15', valid_until: '2026-06-15' });
    expect(intersectValidityWindows([])).toEqual({ valid_from: null, valid_until: null });
    expect(() => intersectValidityWindows([
      { valid_from: '2026-02-30', valid_until: null }
    ])).toThrow('Invalid validity-window start');
    expect(() => intersectValidityWindows([
      { valid_from: '2026-07-01', valid_until: null },
      { valid_from: null, valid_until: '2026-06-30' }
    ])).toThrow('Validity windows do not overlap');
  });
});
