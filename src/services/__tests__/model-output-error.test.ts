import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { ModelOutputContractError, isSafeModelOutputIssuePath } from '../model-output-error';

const canary = 'PRIVATE_MEMORY_PROVIDER_VALUE_SENTINEL';

describe('bounded model-output contract diagnostics', () => {
  it('reports known paths and categories without enum values or unknown object keys', () => {
    const result = z.object({ facts: z.array(z.object({ type: z.enum(['decision']) }).strict()) }).strict()
      .safeParse({ facts: [{ type: canary, [canary]: canary }] });
    expect(result.success).toBe(false);
    if (result.success) throw new Error('Invalid fixture');
    const error = new ModelOutputContractError('extraction', 'schema', result.error.issues);
    expect(error.message).toContain('Invalid extraction output contract');
    expect(error.message).toContain('type:invalid_enum_value');
    expect(error.issues).toEqual([
      { path: 'facts.[].type', code: 'invalid_enum_value' },
      { path: 'facts.[]', code: 'unrecognized_keys' }
    ]);
    expect(error.issueCount).toBe(2);
    expect(JSON.stringify(error)).not.toContain(canary);
    expect(error.cause).toBeUndefined();
  });

  it('does not retain custom refinement messages or attacker-controlled path names', () => {
    const result = z.object({ subject: z.string() }).superRefine((_value, context) => {
      context.addIssue({ code: 'custom', message: canary, path: [canary] });
    }).safeParse({ subject: canary });
    if (result.success) throw new Error('Invalid fixture');
    const error = new ModelOutputContractError('curation', 'schema', result.error.issues);
    expect(error.issues).toEqual([{ path: 'unknown_field', code: 'custom' }]);
    expect(JSON.stringify(error)).not.toContain(canary);
  });

  it('bounds issue count and path depth, replacing all numeric indices and unknown codes', () => {
    const issues = Array.from({ length: 20 }, () => ({
      code: canary, path: ['update', 123456, 'memory', ...Array(20).fill(canary)], message: canary
    } as unknown as z.ZodIssue));
    const error = new ModelOutputContractError('curation', 'schema', issues);
    expect(error.issueCount).toBe(20);
    expect(error.issues).toHaveLength(8);
    expect(error.issues.every(issue => issue.code === 'invalid_value'
      && issue.path.length <= 128 && isSafeModelOutputIssuePath(issue.path))).toBe(true);
    expect(error.issues[0].path).toContain('update.[].memory');
    expect(error.issues[0].path).toContain('truncated_path');
    expect(JSON.stringify(error)).not.toContain(canary);
    expect(JSON.stringify(error)).not.toContain('123456');
    expect(Object.isFrozen(error.issues)).toBe(true);
    expect(Object.isFrozen(error.issues[0])).toBe(true);
  });

  it('uses fixed JSON/completion diagnostics instead of parser or provider causes', () => {
    const messages = {
      extraction: {
        json: 'Invalid extraction JSON', completion: 'Extraction response did not complete',
        truncated: 'Extraction response was truncated', refusal: 'Extraction response was refused'
      },
      curation: {
        json: 'Invalid curator response JSON', completion: 'Curator response did not complete',
        truncated: 'Curator response was truncated', refusal: 'Curator response was refused'
      }
    } as const;
    let parserError: unknown;
    try { JSON.parse(`{"${canary}":`); } catch (error) { parserError = error; }
    expect(parserError).toBeInstanceOf(SyntaxError);
    for (const operation of ['extraction', 'curation'] as const) {
      for (const stage of ['json', 'completion', 'truncated', 'refusal'] as const) {
        const error = new ModelOutputContractError(operation, stage);
        expect(error.message).toBe(messages[operation][stage]);
        expect(error.issues).toEqual([]);
        expect(error.issueCount).toBe(0);
        expect(error.cause).toBeUndefined();
        expect(JSON.stringify(error)).not.toContain(canary);
      }
    }
  });

  it('allows only fixed path vocabulary on subsequent metadata passes', () => {
    expect(isSafeModelOutputIssuePath('scope_changes.[].source_refs.[]')).toBe(true);
    for (const path of [canary, 'facts.123.type', 'facts..type', 'facts.__proto__', 'facts.[]\n',
      Array(9).fill('facts').join('.'), '', null]) {
      expect(isSafeModelOutputIssuePath(path)).toBe(false);
    }
  });
});
