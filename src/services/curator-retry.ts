const CURATOR_TRUNCATION_MARKER = 'curator_output_truncated';
const CURATOR_REQUEST_HASH_PATTERN = /^[0-9a-f]{64}$/;

export interface CuratorRetryLimits {
  candidateLimit: number;
  activeMemoryLimit: number;
  previousTruncatedHash: string | null;
}

type CuratorRetryMode = 'truncated' | 'continuation';

interface CuratorTruncationState {
  mode: CuratorRetryMode;
  requestHash: string;
  candidateCount: number;
  activeMemoryCount: number;
}

function curatorRetryLastError(
  mode: CuratorRetryMode,
  requestHash: string,
  candidateCount: number,
  activeMemoryCount: number
): string {
  if (!CURATOR_REQUEST_HASH_PATTERN.test(requestHash)) throw new Error('Invalid curator request hash');
  if (![candidateCount, activeMemoryCount].every(value => Number.isSafeInteger(value) && value >= 0)) {
    throw new Error('Invalid curator truncation counts');
  }
  return `${CURATOR_TRUNCATION_MARKER}:${mode}:${requestHash}:${candidateCount}:${activeMemoryCount}`;
}

export function curatorTruncationLastError(requestHash: string, candidateCount: number, activeMemoryCount: number): string {
  return curatorRetryLastError('truncated', requestHash, candidateCount, activeMemoryCount);
}

export function curatorRetryContinuationLastError(requestHash: string, candidateCount: number, activeMemoryCount: number): string {
  return curatorRetryLastError('continuation', requestHash, candidateCount, activeMemoryCount);
}

export function parseCuratorTruncationLastError(lastError: string | null): CuratorTruncationState | null {
  if (!lastError?.startsWith(`${CURATOR_TRUNCATION_MARKER}:`)) return null;
  const [mode, requestHash, candidateText, activeMemoryText, ...extra] = lastError.slice(CURATOR_TRUNCATION_MARKER.length + 1).split(':');
  const candidateCount = Number(candidateText);
  const activeMemoryCount = Number(activeMemoryText);
  return extra.length === 0 && (mode === 'truncated' || mode === 'continuation') && CURATOR_REQUEST_HASH_PATTERN.test(requestHash)
    && Number.isSafeInteger(candidateCount) && candidateCount >= 0
    && Number.isSafeInteger(activeMemoryCount) && activeMemoryCount >= 0
    ? { mode, requestHash, candidateCount, activeMemoryCount }
    : null;
}

function halvedLimit(configuredLimit: number, previousCount: number): number {
  if (configuredLimit <= 0 || previousCount <= 0) return 0;
  return Math.min(configuredLimit, Math.max(1, Math.floor(previousCount / 2)));
}

export function getCuratorRetryLimits(input: {
  candidateLimit: number;
  activeMemoryLimit: number;
  lastError: string | null;
}): CuratorRetryLimits {
  const previous = parseCuratorTruncationLastError(input.lastError);
  if (!previous) return {
    candidateLimit: input.candidateLimit,
    activeMemoryLimit: input.activeMemoryLimit,
    previousTruncatedHash: null
  };
  return {
    candidateLimit: previous.mode === 'truncated'
      ? halvedLimit(input.candidateLimit, previous.candidateCount)
      : Math.min(input.candidateLimit, previous.candidateCount),
    activeMemoryLimit: previous.mode === 'truncated'
      ? halvedLimit(input.activeMemoryLimit, previous.activeMemoryCount)
      : Math.min(input.activeMemoryLimit, previous.activeMemoryCount),
    previousTruncatedHash: previous.requestHash
  };
}
