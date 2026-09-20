import { describe, expect, it } from 'vitest';
import { provenanceIdentitySchema } from './provenance-identity';

describe('bounded provenance identity validation', () => {
  it('matches code-point limits and rejects the complete Unicode control class', () => {
    for (const limit of [256, 512]) {
      const schema = provenanceIdentitySchema(limit);
      expect(schema.parse('😀'.repeat(limit))).toHaveLength(limit * 2);
      expect(schema.safeParse('😀'.repeat(limit + 1)).success).toBe(false);
      for (const value of [' ', '\u00a0', '\u0085x', 'x\u009f', 'x\u0000', '\ud800']) {
        expect(schema.safeParse(value).success).toBe(false);
      }
      expect(schema.parse(' valid ')).toBe('valid');
    }
  });
  it('rejects oversized inputs without visiting their code points or trimming them', () => {
    const schema = provenanceIdentitySchema();
    const huge = 'x'.repeat(24 * 1024 * 1024);
    const iterator = String.prototype[Symbol.iterator];
    const trim = String.prototype.trim;
    let visited = false;
    String.prototype[Symbol.iterator] = function () { if (this.length > 1024) visited = true; return iterator.call(this); };
    String.prototype.trim = function () { if (this.length > 1024) visited = true; return trim.call(this); };
    try { expect(schema.safeParse(huge).success).toBe(false); }
    finally { String.prototype[Symbol.iterator] = iterator; String.prototype.trim = trim; }
    expect(visited).toBe(false);
  });
});

describe('shared Unicode string resource bound', () => {
  it('bounds label and context work before downstream regex or trimming', async () => {
    const { captureProvenanceSchema } = await import('./ingest-provenance-schema');
    const { contextIdentitySchema } = await import('./memory-applicability');
    const huge = '😀'.repeat(1024 * 1024);
    const iterator = String.prototype[Symbol.iterator];
    const trim = String.prototype.trim;
    const test = RegExp.prototype.test;
    let visited = false;
    String.prototype[Symbol.iterator] = function () { if (this.length > 1024) visited = true; return iterator.call(this); };
    String.prototype.trim = function () { if (this.length > 1024) visited = true; return trim.call(this); };
    RegExp.prototype.test = function (value) { if (value.length > 1024) visited = true; return test.call(this,value); };
    try {
      const base = { actor_type:'agent',trigger_type:'api',artifact_type:'message',authorship:'generated',cadence:'one_off' };
      expect(contextIdentitySchema.safeParse(huge).success).toBe(false);
      expect(captureProvenanceSchema.safeParse({...base,source_class:huge}).success).toBe(false);
      expect(captureProvenanceSchema.safeParse({...base,provenance_basis:[huge]}).success).toBe(false);
      expect(captureProvenanceSchema.safeParse({...base,provenance_basis:Array(9).fill(huge)}).success).toBe(false);
    } finally {
      String.prototype[Symbol.iterator] = iterator;String.prototype.trim = trim;RegExp.prototype.test = test;
    }
    expect(visited).toBe(false);
  });
});
