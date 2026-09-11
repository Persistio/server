import type { MemoryPolicyRejection } from './memory-evidence';

export type ExtractedMemoryStatus = 'candidate' | 'needs_review';

/**
 * Extraction is evidence gathering, never activation. Curation availability,
 * model output, and extractor self-declared status cannot widen this result.
 */
export function deriveExtractionMemoryStatus(
  policyRejections: MemoryPolicyRejection[]
): ExtractedMemoryStatus {
  return policyRejections.length > 0 ? 'needs_review' : 'candidate';
}
