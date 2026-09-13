import type { z } from 'zod';

export type ModelOutputOperation = 'extraction' | 'curation';
export type ModelOutputStage = 'schema' | 'json' | 'completion' | 'truncated' | 'refusal';
export interface SafeModelOutputIssue { readonly path: string; readonly code: string }

export const MAX_MODEL_OUTPUT_ISSUES = 8;
const MAX_PATH_SEGMENTS = 8;
const MAX_PATH_LENGTH = 128;
const fields = new Set([
  'facts', 'fact', 'subject', 'score', 'salience', 'sensitivity', 'type', 'scope', 'polarity',
  'volatility', 'evidence', 'scope_basis', 'source_refs', 'valid_from', 'valid_until',
  'schema_version', 'keep', 'update', 'consolidate', 'archive', 'edges', 'scope_changes',
  'id', 'reason', 'memory', 'sources', 'basis', 'from', 'to', 'confidence', 'scope_key'
]);
const pathMarkers = new Set(['root', '[]', 'unknown_field', 'truncated_path']);
const issueCodes = new Set([
  'invalid_type', 'invalid_literal', 'custom', 'invalid_union', 'invalid_union_discriminator',
  'invalid_enum_value', 'unrecognized_keys', 'invalid_arguments', 'invalid_return_type',
  'invalid_date', 'invalid_string', 'too_small', 'too_big', 'invalid_intersection_types',
  'not_multiple_of', 'not_finite', 'invalid_value'
]);

/** Revalidate flattened diagnostics when a log passes through multiple exporters. */
export function isSafeModelOutputIssuePath(value: unknown): value is string {
  if (typeof value !== 'string' || value.length === 0 || value.length > MAX_PATH_LENGTH) return false;
  const segments = value.split('.');
  return segments.length <= MAX_PATH_SEGMENTS
    && segments.every(segment => fields.has(segment) || pathMarkers.has(segment));
}

export function isSafeModelOutputIssueCode(value: unknown): value is string {
  return typeof value === 'string' && issueCodes.has(value);
}

function safePath(path: readonly (string | number)[]): string {
  if (path.length === 0) return 'root';
  const segments = path.slice(0, MAX_PATH_SEGMENTS).map(part => typeof part === 'number'
    ? '[]' : fields.has(part) ? part : 'unknown_field');
  if (path.length > MAX_PATH_SEGMENTS) segments[MAX_PATH_SEGMENTS - 1] = 'truncated_path';
  // Segment count and fixed field vocabulary bound this below MAX_PATH_LENGTH.
  return segments.join('.');
}

function message(operation: ModelOutputOperation, stage: ModelOutputStage): string {
  const label = operation === 'extraction' ? 'Extraction' : 'Curator';
  switch (stage) {
    case 'schema': return `Invalid ${operation === 'extraction' ? 'extraction' : 'curator'} output contract`;
    case 'json': return operation === 'extraction' ? 'Invalid extraction JSON' : 'Invalid curator response JSON';
    case 'completion': return `${label} response did not complete`;
    case 'truncated': return `${label} response was truncated`;
    case 'refusal': return `${label} response was refused`;
  }
}

/** Carries only fixed metadata, never Zod messages, values, unknown keys or causes. */
export class ModelOutputContractError extends Error {
  readonly issues: readonly SafeModelOutputIssue[];
  readonly issueCount: number;

  constructor(
    readonly operation: ModelOutputOperation,
    readonly stage: ModelOutputStage,
    issues: readonly z.ZodIssue[] = []
  ) {
    const safeIssues = issues.slice(0, MAX_MODEL_OUTPUT_ISSUES).map(issue => Object.freeze({
      path: safePath(issue.path),
      code: isSafeModelOutputIssueCode(issue.code) ? issue.code : 'invalid_value'
    }));
    // Existing queue and private review audit records persist Error.message.
    // Keep the same safe diagnostics there, not just in transient object fields.
    const summary = safeIssues.map(issue => `${issue.path}:${issue.code}`).join('; ');
    super(message(operation, stage) + (summary ? ` (${summary})` : ''));
    this.name = 'ModelOutputContractError';
    this.issueCount = issues.length;
    this.issues = Object.freeze(safeIssues);
  }
}
