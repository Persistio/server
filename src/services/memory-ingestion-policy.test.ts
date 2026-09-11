import { describe, expect, it } from 'vitest';

import { deriveExtractionMemoryStatus } from './memory-ingestion-policy';

describe('deriveExtractionMemoryStatus', () => {
  it('keeps policy-clean extracted memories as candidates regardless of curator scheduling', () => {
    expect(deriveExtractionMemoryStatus([])).toBe('candidate');
  });

  it('quarantines any extraction policy rejection', () => {
    expect(deriveExtractionMemoryStatus([{
      code: 'untrusted_provenance',
      field: 'provenance',
      reason: 'imported'
    }])).toBe('needs_review');
  });
});
