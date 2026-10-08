import { describe, expect, it } from 'vitest';
import { curatorRetryContinuationLastError, curatorTruncationLastError, getCuratorRetryLimits, parseCuratorTruncationLastError } from '../curator-retry';

const hash = 'a'.repeat(64);

describe('curator truncation retries', () => {
  it('halves candidates and reviewed context on each paid truncation retry', () => {
    const attempts = [[20,80],[10,40],[5,20],[2,10]];
    expect(attempts.map(([candidateCount,activeMemoryCount]) => getCuratorRetryLimits({
      candidateLimit: 20, activeMemoryLimit: 80,
      lastError: curatorTruncationLastError(hash, candidateCount, activeMemoryCount)
    }))).toEqual([
      { candidateLimit: 10, activeMemoryLimit: 40, previousTruncatedHash: hash },
      { candidateLimit: 5, activeMemoryLimit: 20, previousTruncatedHash: hash },
      { candidateLimit: 2, activeMemoryLimit: 10, previousTruncatedHash: hash },
      { candidateLimit: 1, activeMemoryLimit: 5, previousTruncatedHash: hash }
    ]);
  });

  it('keeps configured limits after every other failure type', () => {
    expect(getCuratorRetryLimits({ candidateLimit: 20, activeMemoryLimit: 80, lastError: 'provider unavailable' }))
      .toEqual({ candidateLimit: 20, activeMemoryLimit: 80, previousTruncatedHash: null });
  });

  it('retries the current reduced batch after a non-truncation failure without shrinking or resetting it', () => {
    expect(getCuratorRetryLimits({ candidateLimit: 20, activeMemoryLimit: 80,
      lastError: curatorRetryContinuationLastError(hash, 5, 20) }))
      .toEqual({ candidateLimit: 5, activeMemoryLimit: 20, previousTruncatedHash: hash });
  });

  it('accepts only the bounded structural truncation marker', () => {
    expect(parseCuratorTruncationLastError(curatorTruncationLastError(hash,20,80)))
      .toEqual({mode:'truncated',requestHash:hash,candidateCount:20,activeMemoryCount:80});
    expect(parseCuratorTruncationLastError('curator_output_truncated:not-a-hash')).toBeNull();
    expect(() => curatorTruncationLastError('not-a-hash',20,80)).toThrow('Invalid curator request hash');
  });
});
