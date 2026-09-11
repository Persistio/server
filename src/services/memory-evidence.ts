export interface MemoryPolicyRejection {
  code: string;
  field: string;
  reason: string;
}

export function mergeMemoryEvidence(
  existingEvidence: unknown,
  summary: string | null | undefined,
  policyRejections: MemoryPolicyRejection[] = []
): string | null {
  const existing = parseEvidence(existingEvidence);
  const existingRecord = isEvidenceRecord(existing) ? existing : {};
  const nextSummary = summary === undefined
    ? typeof existingRecord.summary === 'string'
      ? existingRecord.summary
      : typeof existing === 'string' ? existing : null
    : summary;
  const mergedPolicyRejections = Array.from(
    new Map([
      ...readPolicyRejections(existing),
      ...policyRejections
    ].map((rejection) => [
      `${rejection.code}:${rejection.field}:${rejection.reason}`,
      rejection
    ])).values()
  );
  const result: Record<string, unknown> = {
    ...existingRecord,
    summary: nextSummary
  };
  if (mergedPolicyRejections.length > 0) {
    result.policy_rejections = mergedPolicyRejections;
  }

  if (nextSummary === null && Object.keys(result).every((key) => key === 'summary')) {
    return null;
  }
  return JSON.stringify(result);
}

function parseEvidence(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function isEvidenceRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readPolicyRejections(value: unknown): MemoryPolicyRejection[] {
  if (!isEvidenceRecord(value) || !Array.isArray(value.policy_rejections)) return [];
  return value.policy_rejections.filter((rejection): rejection is MemoryPolicyRejection => (
    isEvidenceRecord(rejection) &&
    typeof rejection.code === 'string' &&
    typeof rejection.field === 'string' &&
    typeof rejection.reason === 'string'
  ));
}
