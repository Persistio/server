import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../../db/client', () => ({
  query: queryMock,
  withTransaction: async (callback: (client: { query: typeof queryMock }) => Promise<unknown>) => callback({ query: queryMock })
}));

import {
  AiBudgetDeferredError,
  QuotaExceededError,
  acquireAiBudget,
  consumeApiQuota,
  consumeNormalIngestRateLimit,
  incrementUsage,
  recordModelUsage,
  refundApiQuotaReservation,
  reserveApiQuota,
  settleAiUsage,
  usageTestInternals
} from '../usage';

const noClosingUsagePeriod = { rowCount: 0, rows: [] };

describe('usage service', () => {
  beforeEach(() => {
    queryMock.mockReset();
    usageTestInternals.clearAiBuckets();
    vi.useRealTimers();
  });

  describe('consumeApiQuota', () => {
    it('seeds unlimited plan quotas and memory capacity as positive guardrails', () => {
      const migration = readFileSync(resolve(__dirname, '../../db/migrations/002_rename_tenant_to_vault.sql'), 'utf8');
      const unlimitedLimitsJson = migration.match(/\('unlimited', '([^']+)'\)/)?.[1];
      expect(unlimitedLimitsJson).toBeDefined();

      const unlimitedLimits = JSON.parse(unlimitedLimitsJson ?? '{}') as Record<string, number>;
      expect(unlimitedLimits.memories_max).toBeGreaterThan(0);
      expect(unlimitedLimits.ingest_events_per_month).toBeGreaterThan(0);
      expect(unlimitedLimits.memory_adds_per_month).toBeGreaterThan(0);
      expect(unlimitedLimits.searches_per_month).toBeGreaterThan(0);
    });

    it('throws when the atomic consume is rejected by the quota guard', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          quota_limit: '5'
        }]
      });
      queryMock.mockResolvedValueOnce(noClosingUsagePeriod);
      queryMock.mockResolvedValueOnce({
        rowCount: 0,
        rows: []
      });

      await expect(consumeApiQuota('vault-1', 'memory_adds')).rejects.toMatchObject({
        name: 'QuotaExceededError',
        headers: expect.objectContaining({
          limit: 5,
          remaining: 0
        })
      });
      expect(queryMock).toHaveBeenCalledTimes(3);
      expect(queryMock.mock.calls[1]?.[0]).toContain('FOR UPDATE OF vu');
      expect(queryMock.mock.calls[2]?.[0]).toContain('vault_usage.memory_adds < $6::int');
    });

    it('returns remaining quota after a successful atomic consume', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          quota_limit: '10'
        }]
      });
      queryMock.mockResolvedValueOnce(noClosingUsagePeriod);
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
      expect(queryMock).toHaveBeenCalledTimes(3);
      expect(queryMock.mock.calls[0]?.[0]).not.toContain('FOR UPDATE');
      expect(queryMock.mock.calls[0]?.[0]).toContain('FOR KEY SHARE OF v');
      expect(queryMock.mock.calls[2]?.[1]).toEqual([
        'vault-1',
        expect.stringMatching(/^\d{4}-\d{2}$/),
        0,
        1,
        0,
        10
      ]);
    });

    it('returns a reservation that can be refunded against the reserved period', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          quota_limit: '10'
        }]
      });
      queryMock.mockResolvedValueOnce(noClosingUsagePeriod);
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          consumed: '3'
        }]
      });
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: []
      });

      const reservation = await reserveApiQuota('vault-1', 'ingest_events');
      expect(reservation).toMatchObject({
        field: 'ingest_events',
        period: expect.stringMatching(/^\d{4}-\d{2}$/),
        snapshot: {
          limit: 10,
          remaining: 7,
          retryAfterSeconds: null
        },
        vaultId: 'vault-1'
      });

      await refundApiQuotaReservation(reservation);

      expect(queryMock).toHaveBeenCalledTimes(4);
      expect(queryMock.mock.calls[3]?.[0]).toContain('SET ingest_events = GREATEST(ingest_events - 1, 0)');
      expect(queryMock.mock.calls[3]?.[0]).toContain('AND period = $2');
      expect(queryMock.mock.calls[3]?.[1]).toEqual(['vault-1', reservation.period]);
    });

    it('writes a closed-period event before rolling usage into a new period', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-06-01T00:00:03.000Z'));
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          quota_limit: '4'
        }]
      });
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          account_id: 'account-1',
          curator_candidates_deferred: '2',
          curator_candidates_processed: '9',
          curator_input_tokens: '100',
          curator_output_tokens: '20',
          curator_requests: '4',
          curator_runs: '1',
          ingest_events: '7',
          limits: {
            ingest_events_per_month: 10,
            memory_adds_per_month: 20,
            searches_per_month: 30,
            curator_runs_per_month: 5
          },
          memory_adds: '3',
          period: '2026-05',
          plan_id: 'unlimited',
          rate_limit_override: {
            searches_per_month: 4,
            curator_requests_per_month: 8,
            curator_tokens_per_month: 1000
          },
          searches: '2'
        }]
      });
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: []
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
      expect(queryMock).toHaveBeenCalledTimes(4);
      expect(queryMock.mock.calls[2]?.[0]).toContain('INSERT INTO platform_event_outbox');
      expect(queryMock.mock.calls[2]?.[1]?.[0]).toBe('vault:vault-1');
      expect(JSON.parse(queryMock.mock.calls[2]?.[1]?.[1] as string)).toEqual({
        platform_vault_id: 'vault-1',
        account_id: 'account-1',
        period: '2026-05',
        plan_id: 'unlimited',
        usage: {
          ingest_events: 7,
          memory_adds: 3,
          searches: 2,
          curator_runs: 1,
          curator_requests: 4,
          curator_input_tokens: 100,
          curator_output_tokens: 20,
          curator_candidates_processed: 9,
          curator_candidates_deferred: 2
        },
        limits: {
          ingest_events_per_month: 10,
          memory_adds_per_month: 20,
          searches_per_month: 4,
          curator_runs_per_month: 5,
          curator_requests_per_month: 8,
          curator_tokens_per_month: 1000
        }
      });
      expect(queryMock.mock.calls[3]?.[1]).toEqual([
        'vault-1',
        '2026-06',
        0,
        0,
        1,
        4
      ]);
    });

    it('does not roll over usage when the closed-period event insert fails', async () => {
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          quota_limit: '4'
        }]
      });
      queryMock.mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          account_id: null,
          curator_candidates_deferred: '0',
          curator_candidates_processed: '0',
          curator_input_tokens: '0',
          curator_output_tokens: '0',
          curator_requests: '0',
          curator_runs: '0',
          ingest_events: '1',
          limits: null,
          memory_adds: '0',
          period: '2026-05',
          plan_id: 'free',
          rate_limit_override: null,
          searches: '0'
        }]
      });
      queryMock.mockRejectedValueOnce(new Error('outbox unavailable'));

      await expect(consumeApiQuota('vault-1', 'ingest_events')).rejects.toThrow('outbox unavailable');

      expect(queryMock).toHaveBeenCalledTimes(3);
      expect(queryMock.mock.calls[2]?.[0]).toContain('INSERT INTO platform_event_outbox');
    });
  });

  describe('incrementUsage', () => {
    it('checks for a closed period in the same transaction before incrementing', async () => {
      queryMock.mockResolvedValueOnce(noClosingUsagePeriod);
      queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [] });

      await incrementUsage('vault-1', 'memory_adds');

      expect(queryMock).toHaveBeenCalledTimes(2);
      expect(queryMock.mock.calls[0]?.[0]).toContain('FOR UPDATE OF vu');
      expect(queryMock.mock.calls[1]?.[0]).toContain('INSERT INTO vault_usage');
      expect(queryMock.mock.calls[1]?.[1]).toEqual([
        'vault-1',
        expect.stringMatching(/^\d{4}-\d{2}$/),
        0,
        1,
        0
      ]);
    });
  });

  describe('recordModelUsage', () => {
    it('rolls up durable per-vault provider/model-role usage', async () => {
      queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [] });

      await recordModelUsage({
        vaultId: 'vault-1',
        provider: 'google',
        modelRole: 'extraction',
        model: 'gemini-2.5-flash',
        requestCount: 1,
        promptTokens: 100,
        completionTokens: 25,
        totalTokens: 125
      });

      expect(queryMock).toHaveBeenCalledTimes(1);
      expect(queryMock.mock.calls[0]?.[0]).toContain('INSERT INTO vault_model_usage');
      expect(queryMock.mock.calls[0]?.[0]).toContain('ON CONFLICT (vault_id, period, provider, model_role, model)');
      expect(queryMock.mock.calls[0]?.[1]).toEqual([
        'vault-1',
        expect.stringMatching(/^\d{4}-\d{2}$/),
        'google',
        'extraction',
        'gemini-2.5-flash',
        1,
        0,
        0,
        0,
        100,
        25,
        125
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

    it('calculates wait time when a token bucket is saturated', () => {
      const buckets = new Map<string, {
        capacity: number;
        lastRefillMs: number;
        refillPerMs: number;
        tokens: number;
      }>();

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      expect(usageTestInternals.getTokenBucketWaitMs(buckets, 'vault-1:tokens', 100, 80)).toBe(0);
      buckets.set('vault-1:tokens', { capacity: 100, lastRefillMs: Date.now(), refillPerMs: 100 / 60_000, tokens: 20 });
      expect(buckets.get('vault-1:tokens')?.tokens).toBe(20);
      expect(usageTestInternals.getTokenBucketWaitMs(buckets, 'vault-1:tokens', 100, 30)).toBe(6000);
    });

    it('defers instead of throwing a quota error when local AI budget is saturated', async () => {
      queryMock.mockResolvedValue({
        rowCount: 1,
        rows: [{
          plan_id: 'free',
          limits: null,
          rate_limit_override: {
            ai_tokens_per_minute: 100
          }
        }]
      });

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      await acquireAiBudget('vault-1', 'extraction', 80);
      await expect(acquireAiBudget('vault-1', 'extraction', 30)).rejects.toBeInstanceOf(AiBudgetDeferredError);
    });

    it('uses conservative AI defaults for unknown plan IDs', async () => {
      queryMock.mockResolvedValue({
        rowCount: 1,
        rows: [{
          plan_id: 'custom',
          limits: null,
          rate_limit_override: null
        }]
      });

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      await acquireAiBudget('vault-1', 'extraction', 40_000);
      await expect(acquireAiBudget('vault-1', 'extraction', 20_000)).rejects.toBeInstanceOf(AiBudgetDeferredError);
    });

    it('keeps vault buckets independent for fair per-vault throttling', async () => {
      queryMock.mockResolvedValue({
        rowCount: 1,
        rows: [{
          plan_id: 'free',
          limits: null,
          rate_limit_override: { ai_tokens_per_minute: 100 }
        }]
      });

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      await acquireAiBudget('vault-1', 'extraction', 90);
      await acquireAiBudget('vault-2', 'extraction', 90);

      expect(usageTestInternals.getAiBucketTokens('tokens', 'vault-1', 'extraction')).toBe(10);
      expect(usageTestInternals.getAiBucketTokens('tokens', 'vault-2', 'extraction')).toBe(10);
    });

    it('keeps role buckets separate and charges weighted token overage during settlement', async () => {
      queryMock.mockResolvedValue({
        rowCount: 1,
        rows: [{
          plan_id: 'free',
          limits: null,
          rate_limit_override: {
            ai_tokens_per_minute: 100,
            ai_escalation_weight: 2
          }
        }]
      });

      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));
      await acquireAiBudget('vault-1', 'extraction', 60);
      await acquireAiBudget('vault-1', 'escalation', 30);
      expect(usageTestInternals.getAiBucketTokens('tokens', 'vault-1', 'extraction')).toBe(40);
      expect(usageTestInternals.getAiBucketTokens('tokens', 'vault-1', 'escalation')).toBe(40);

      await settleAiUsage('vault-1', 'escalation', 30, 45);
      expect(usageTestInternals.getAiBucketTokens('tokens', 'vault-1', 'escalation')).toBe(10);
      expect(queryMock).toHaveBeenCalledTimes(3);
    });
  });

  describe('normal ingest request throttling', () => {
    it('rate limits non-premium normal ingest requests per minute', () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));

      expect(consumeNormalIngestRateLimit('vault-1', 'free', 1)).toMatchObject({
        limit: 1,
        remaining: 0,
        retryAfterSeconds: null
      });

      expect(() => consumeNormalIngestRateLimit('vault-1', 'free', 1)).toThrow(QuotaExceededError);
    });

    it('bypasses normal ingest RPM throttling for premium plans', () => {
      expect(consumeNormalIngestRateLimit('vault-1', 'unlimited', 1)).toEqual({
        limit: null,
        remaining: null,
        resetAtEpochSeconds: null,
        retryAfterSeconds: null
      });
      expect(usageTestInternals.getIngestBucketTokens('vault-1')).toBeNull();
    });

    it('does not treat legacy pro as a premium-plan alias', () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-05-12T12:26:00.000Z'));

      expect(consumeNormalIngestRateLimit('vault-1', 'pro', 1)).toMatchObject({
        limit: 1,
        remaining: 0,
        retryAfterSeconds: null
      });
      expect(() => consumeNormalIngestRateLimit('vault-1', 'pro', 1)).toThrow(QuotaExceededError);
    });
  });
});
