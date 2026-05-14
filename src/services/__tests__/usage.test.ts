import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../../db/client', () => ({
  query: queryMock,
  withTransaction: async (callback: (client: { query: typeof queryMock }) => Promise<unknown>) => callback({ query: queryMock })
}));

import { QuotaExceededError, consumeApiQuota, consumeGeminiQuota, settleGeminiUsage, usageTestInternals } from '../usage';

describe('usage service', () => {
  const currentPeriod = new Date().toISOString().slice(0, 7);

  beforeEach(() => {
    queryMock.mockReset();
    usageTestInternals.clearGeminiBuckets();
    vi.useRealTimers();
  });

  describe('consumeApiQuota', () => {
    it('throws before writing when the current usage is already at the quota limit', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          current_period: currentPeriod,
          current_value: '5',
          quota_limit: '5'
        }]
      });

      await expect(consumeApiQuota('vault-1', 'memory_adds')).rejects.toMatchObject({
        name: 'QuotaExceededError',
        headers: expect.objectContaining({
          limit: 5,
          remaining: 0
        })
      });
      expect(queryMock).toHaveBeenCalledTimes(1);
    });

    it('returns remaining quota after a successful atomic consume', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          current_period: currentPeriod,
          current_value: '2',
          quota_limit: '10'
        }]
      });
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          consumed: '3'
        }]
      });

      await expect(consumeApiQuota('vault-1', 'memory_adds')).resolves.toMatchObject({
        limit: 10,
        remaining: 7,
        retryAfterSeconds: null
      });
      expect(queryMock).toHaveBeenCalledTimes(2);
    });

    it('treats a new period as rolled over and reports the fresh remaining quota', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          current_period: '2026-04',
          current_value: '9',
          quota_limit: '4'
        }]
      });
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          consumed: '1'
        }]
      });

      await expect(consumeApiQuota('vault-1', 'searches')).resolves.toMatchObject({
        limit: 4,
        remaining: 3
      });
      expect(queryMock).toHaveBeenCalledTimes(2);
      expect(queryMock.mock.calls[1]?.[1]).toEqual([
        'vault-1',
        expect.stringMatching(/^\d{4}-\d{2}$/),
        0,
        0,
        1
      ]);
    });
  });

  describe('token bucket helpers', () => {
    it('refills elapsed capacity up to the bucket limit', () => {
      const refilled = usageTestInternals.refillBucket({
        capacity: 120,
        lastRefillMs: 10_000,
        refillPerMs: 120 / 60_000,
        tokens: 20
      }, 120, 25_000);

      expect(refilled.tokens).toBe(50);
      expect(refilled.capacity).toBe(120);
      expect(refilled.lastRefillMs).toBe(25_000);
      expect(refilled.refillPerMs).toBe(120 / 60_000);
    });

    it('tracks deficit when a consume request exceeds remaining tokens', () => {
      const buckets = new Map<string, {
        capacity: number;
        lastRefillMs: number;
        refillPerMs: number;
        tokens: number;
      }>();

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      usageTestInternals.assertAndConsumeTokenBucket(buckets, 'vault-1:tokens', 100, 80, 'quota exceeded');
      expect(buckets.get('vault-1:tokens')?.tokens).toBe(20);

      try {
        usageTestInternals.assertAndConsumeTokenBucket(buckets, 'vault-1:tokens', 100, 30, 'quota exceeded');
        throw new Error('expected quota exceeded');
      } catch (error) {
        expect(error).toBeInstanceOf(QuotaExceededError);
        expect((error as QuotaExceededError).headers).toMatchObject({
          limit: 100,
          remaining: 20,
          retryAfterSeconds: 6
        });
      }
    });

    it('charges additional token overage during settlement', async () => {
      queryMock.mockResolvedValue({
        rowCount: 1,
        rows: [{
          plan_id: 'free',
          limits: null,
          rate_limit_override: {
            gemini_tpm: 100
          }
        }]
      });

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      await consumeGeminiQuota('vault-1', 60);
      expect(usageTestInternals.getGeminiBucketTokens('tokens', 'vault-1')).toBe(40);

      await settleGeminiUsage('vault-1', 60, 90);
      expect(usageTestInternals.getGeminiBucketTokens('tokens', 'vault-1')).toBe(10);
      expect(queryMock).toHaveBeenCalledTimes(2);
    });
  });
});
