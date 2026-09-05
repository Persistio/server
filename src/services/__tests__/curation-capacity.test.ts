import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../../db/client', () => ({
  query: queryMock,
  withTransaction: async (callback: (client: { query: typeof queryMock }) => Promise<unknown>) => callback({ query: queryMock })
}));

import {
  claimEligibleCurationJobs,
  getCurationStatus,
  getCuratorPlanBlockReason,
  mergeCuratorLimits,
  recordCuratorRunCompletedActivity,
  recordCuratorUsage
} from '../curation-capacity';

describe('curation capacity service', () => {
  beforeEach(() => {
    queryMock.mockReset();
    vi.useRealTimers();
  });

  it('merges curator plan limits with vault overrides', () => {
    const limits = mergeCuratorLimits('unlimited', {
      curator_jobs_per_run: 5,
      curator_runs_per_month: 30,
      curator_enabled: true
    }, {
      curator_jobs_per_run: 2,
      curator_overage_mode: 'upgrade_only'
    });

    expect(limits).toMatchObject({
      curator_enabled: true,
      curator_runs_per_month: 30,
      curator_jobs_per_run: 2,
      curator_candidates_per_call: 20,
      curator_overage_mode: 'upgrade_only'
    });
  });

  it('uses the faster unlimited curator cadence and budget envelope', () => {
    expect(mergeCuratorLimits('unlimited', null, null)).toMatchObject({
      curator_enabled: true,
      curator_schedule_interval_minutes: 15,
      curator_runs_per_month: 3000,
      curator_requests_per_month: 6000,
      curator_tokens_per_month: 25000000,
      curator_overage_mode: 'payg'
    });
  });

  it('does not keep legacy pro as an unlimited curator alias', () => {
    expect(mergeCuratorLimits('pro', null, null)).toMatchObject({
      curator_enabled: false,
      curator_runs_per_month: 0,
      curator_jobs_per_run: 0,
      curator_tokens_per_month: 0,
      curator_requests_per_month: 0,
      curator_overage_mode: 'disable'
    });
  });

  it('defaults unknown plans to disabled curator capacity', () => {
    expect(mergeCuratorLimits('custom', null, null)).toMatchObject({
      curator_enabled: false,
      curator_runs_per_month: 0,
      curator_jobs_per_run: 0,
      curator_tokens_per_month: 0,
      curator_requests_per_month: 0,
      curator_overage_mode: 'disable'
    });
  });

  it('allows unknown plans to opt into curator capacity with explicit limits', () => {
    expect(mergeCuratorLimits('custom', {
      curator_enabled: true,
      curator_runs_per_month: 3,
      curator_jobs_per_run: 1,
      curator_requests_per_month: 3,
      curator_tokens_per_month: 10000
    }, null)).toMatchObject({
      curator_enabled: true,
      curator_runs_per_month: 3,
      curator_jobs_per_run: 1,
      curator_requests_per_month: 3,
      curator_tokens_per_month: 10000
    });
  });

  it('returns live queue, usage, schedule, and recent run status', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-05-23T10:00:00.000Z'));
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        plan_id: 'unlimited',
        limits: { curator_enabled: true, curator_backlog_limit: 10 },
        rate_limit_override: { curator_schedule_interval_minutes: 120 },
        curator_runs: '2',
        curator_requests: '3',
        curator_input_tokens: '1000',
        curator_output_tokens: '200',
        curator_candidates_processed: '40',
        curator_candidates_deferred: '5',
        pending_queue_rows: '8',
        available_queue_rows: '8',
        oldest_queue_age_seconds: '3600',
        next_queue_available_at: null,
        last_curator_run_at: '2026-05-23T08:00:00.000Z',
        next_curator_run_at: '2026-05-23T12:00:00.000Z',
        last_curator_defer_reason: null
      }]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        segment_id: 'b0a76dd4-5447-44ed-8d3d-e8859f13f128',
        triggered_at: '2026-05-23T08:10:00.000Z',
        actions: '4',
        applied_actions: '3',
        errors: '1'
      }]
    });

    await expect(getCurationStatus('dff718f2-9d97-43b2-a3cc-a14099ed42c3')).resolves.toMatchObject({
      plan: 'unlimited',
      period: '2026-05',
      usage: {
        curator_runs: 2,
        curator_requests: 3,
        curator_input_tokens: 1000,
        curator_output_tokens: 200,
        curator_candidates_processed: 40,
        curator_candidates_deferred: 5
      },
      backlog: {
        pending_queue_rows: 8,
        oldest_queue_age_seconds: 3600,
        backlog_limit: 10,
        pressure: 'warning'
      },
      schedule: {
        eligible_now: false,
        defer_reason: 'waiting for scheduled curator interval'
      },
      recent_runs: [{
        segment_id: 'b0a76dd4-5447-44ed-8d3d-e8859f13f128',
        actions: 4,
        applied_actions: 3,
        errors: 1
      }]
    });
  });

  it('allows a zero jobs-per-run override to pause curation claiming', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(claimEligibleCurationJobs(5, 'worker-1')).resolves.toEqual([]);

    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining("GREATEST(0, COALESCE((v.rate_limit_override->>'curator_jobs_per_run')"),
      [5, expect.stringMatching(/^\d{4}-\d{2}$/), 'worker-1']
    );
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('WHEN ev.runs_limit IS NULL OR ev.runs_used < ev.runs_limit THEN ev.jobs_per_run'),
      [5, expect.stringMatching(/^\d{4}-\d{2}$/), 'worker-1']
    );
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining("curator_input_tokens_per_call'), (p.limits->>'curator_input_tokens_per_call'), '0')::int > 0"),
      [5, expect.stringMatching(/^\d{4}-\d{2}$/), 'worker-1']
    );
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('reserved_vaults AS'),
      [5, expect.stringMatching(/^\d{4}-\d{2}$/), 'worker-1']
    );
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('ON CONFLICT (vault_id) DO UPDATE'),
      [5, expect.stringMatching(/^\d{4}-\d{2}$/), 'worker-1']
    );
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('JOIN reserved_vaults'),
      [5, expect.stringMatching(/^\d{4}-\d{2}$/), 'worker-1']
    );
  });

  it('can count curator requests without counting a new scheduled run', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [] });

    await recordCuratorUsage({
      vaultId: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      candidatesProcessed: 3,
      countRun: false,
      promptTokens: 100,
      completionTokens: 25,
      limits: mergeCuratorLimits('unlimited', null, null)
    });

    expect(queryMock).toHaveBeenCalledTimes(2);
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('curator_runs = CASE WHEN vault_usage.period = EXCLUDED.period THEN vault_usage.curator_runs + EXCLUDED.curator_runs'),
      [
        'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        expect.stringMatching(/^\d{4}-\d{2}$/),
        0,
        100,
        25,
        3
      ]
    );
  });

  it('writes a closed-period event before curator usage rolls into a new period', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-06-01T00:00:03.000Z'));
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        account_id: null,
        curator_candidates_deferred: '1',
        curator_candidates_processed: '8',
        curator_input_tokens: '900',
        curator_output_tokens: '90',
        curator_requests: '6',
        curator_runs: '3',
        ingest_events: '10',
        limits: {
          curator_runs_per_month: 30,
          curator_requests_per_month: 100,
          curator_tokens_per_month: 100000
        },
        memory_adds: '4',
        period: '2026-05',
        plan_id: 'unlimited',
        rate_limit_override: null,
        searches: '12'
      }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [] });
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [] });

    await recordCuratorUsage({
      vaultId: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      candidatesProcessed: 3,
      countRun: false,
      promptTokens: 100,
      completionTokens: 25,
      limits: mergeCuratorLimits('unlimited', null, null)
    });

    expect(queryMock).toHaveBeenCalledTimes(3);
    expect(queryMock.mock.calls[1]?.[0]).toContain('INSERT INTO platform_event_outbox');
    expect(queryMock.mock.calls[1]?.[1]?.[1]).toBe('io.persistio.usage_period.closed.v1');
    expect(JSON.parse(queryMock.mock.calls[1]?.[1]?.[2] as string)).toMatchObject({
      platform_vault_id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      period: '2026-05',
      usage: {
        curator_runs: 3,
        curator_requests: 6,
        curator_input_tokens: 900,
        curator_output_tokens: 90,
        curator_candidates_processed: 8,
        curator_candidates_deferred: 1
      },
      limits: {
        curator_runs_per_month: 30,
        curator_requests_per_month: 100,
        curator_tokens_per_month: 100000
      }
    });
    expect(queryMock.mock.calls[2]?.[0]).toContain('INSERT INTO vault_usage');
  });

  it('writes one aggregate curator completed activity event', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ account_id: 'workspace-1' }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [] });

    await expect(recordCuratorRunCompletedActivity({
      vaultId: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      totalCandidatesProcessed: 8,
      totalCuratorRequests: 2,
      totalCuratorRuns: 1,
      totalPromptTokens: 300,
      totalCompletionTokens: 75
    })).resolves.toBe(true);

    expect(queryMock).toHaveBeenCalledTimes(2);
    expect(queryMock.mock.calls[1]?.[0]).toContain('INSERT INTO platform_event_outbox');
    expect(queryMock.mock.calls[1]?.[1]?.[0]).toBe('io.persistio.curator.run.completed.v1');
    expect(JSON.parse(queryMock.mock.calls[1]?.[1]?.[2] as string)).toMatchObject({
      candidates_processed: 8,
      counts: {
        candidates_processed: 8,
        curator_requests: 2,
        curator_runs: 1
      },
      curator_input_tokens: 300,
      curator_output_tokens: 75,
      curator_requests: 2,
      platform_vault_id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      workspace_id: 'workspace-1'
    });
  });

  it('treats curator completed activity writes as best effort', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    queryMock.mockRejectedValueOnce(new Error('outbox unavailable'));

    try {
      await expect(recordCuratorRunCompletedActivity({
        vaultId: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        totalCandidatesProcessed: 1,
        totalCuratorRequests: 1,
        totalCuratorRuns: 1,
        totalPromptTokens: 10,
        totalCompletionTokens: 2
      })).resolves.toBe(false);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it('reports zero monthly curator limits as exhausted', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        plan_id: 'unlimited',
        limits: {
          curator_enabled: true,
          curator_runs_per_month: 0,
          curator_requests_per_month: 10,
          curator_tokens_per_month: 1000
        },
        rate_limit_override: null,
        curator_runs: '0',
        curator_requests: '0',
        curator_input_tokens: '0',
        curator_output_tokens: '0',
        curator_candidates_processed: '0',
        curator_candidates_deferred: '0',
        pending_queue_rows: '1',
        available_queue_rows: '1',
        oldest_queue_age_seconds: '60',
        next_queue_available_at: null,
        last_curator_run_at: null,
        next_curator_run_at: null,
        last_curator_defer_reason: null
      }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(getCurationStatus('dff718f2-9d97-43b2-a3cc-a14099ed42c3')).resolves.toMatchObject({
      schedule: {
        eligible_now: false,
        defer_reason: 'monthly curator run limit exhausted'
      }
    });
  });

  it('treats zero per-call token caps as current capacity blockers', () => {
    expect(getCuratorPlanBlockReason(mergeCuratorLimits('unlimited', null, {
      curator_input_tokens_per_call: 0
    }))).toBe('curator input token limit exhausted');

    expect(getCuratorPlanBlockReason(mergeCuratorLimits('unlimited', null, {
      curator_output_tokens_per_call: 0
    }))).toBe('curator output token limit exhausted');
  });

  it('reports zero per-call token caps as not currently eligible', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        plan_id: 'unlimited',
        limits: {
          curator_enabled: true,
          curator_input_tokens_per_call: 0
        },
        rate_limit_override: null,
        curator_runs: '0',
        curator_requests: '0',
        curator_input_tokens: '0',
        curator_output_tokens: '0',
        curator_candidates_processed: '0',
        curator_candidates_deferred: '0',
        pending_queue_rows: '1',
        available_queue_rows: '1',
        oldest_queue_age_seconds: '60',
        next_queue_available_at: null,
        last_curator_run_at: null,
        next_curator_run_at: null,
        last_curator_defer_reason: null
      }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(getCurationStatus('dff718f2-9d97-43b2-a3cc-a14099ed42c3')).resolves.toMatchObject({
      schedule: {
        eligible_now: false,
        defer_reason: 'curator input token limit exhausted'
      }
    });
  });

  it('does not report stale defer reasons when curation is currently eligible', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        plan_id: 'unlimited',
        limits: { curator_enabled: true },
        rate_limit_override: null,
        curator_runs: '0',
        curator_requests: '0',
        curator_input_tokens: '0',
        curator_output_tokens: '0',
        curator_candidates_processed: '0',
        curator_candidates_deferred: '5',
        pending_queue_rows: '1',
        available_queue_rows: '1',
        oldest_queue_age_seconds: '60',
        next_queue_available_at: null,
        last_curator_run_at: null,
        next_curator_run_at: null,
        last_curator_defer_reason: 'curator candidate limit exhausted'
      }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    await expect(getCurationStatus('dff718f2-9d97-43b2-a3cc-a14099ed42c3')).resolves.toMatchObject({
      schedule: {
        eligible_now: true,
        defer_reason: null
      }
    });
  });

  it('reports pending future-available queue rows as not currently eligible', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        plan_id: 'unlimited',
        limits: { curator_enabled: true },
        rate_limit_override: null,
        curator_runs: '0',
        curator_requests: '0',
        curator_input_tokens: '0',
        curator_output_tokens: '0',
        curator_candidates_processed: '0',
        curator_candidates_deferred: '0',
        pending_queue_rows: '1',
        available_queue_rows: '0',
        oldest_queue_age_seconds: '60',
        next_queue_available_at: '2026-05-23T10:30:00.000Z',
        last_curator_run_at: null,
        next_curator_run_at: '2026-05-23T10:10:00.000Z',
        last_curator_defer_reason: null
      }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-05-23T10:20:00.000Z'));
    await expect(getCurationStatus('dff718f2-9d97-43b2-a3cc-a14099ed42c3')).resolves.toMatchObject({
      schedule: {
        next_run_at: '2026-05-23T10:30:00.000Z',
        eligible_now: false,
        defer_reason: 'waiting for queued curation availability'
      }
    });
  });

  it('reports vault claim leases as not currently eligible', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        plan_id: 'unlimited',
        limits: { curator_enabled: true },
        rate_limit_override: null,
        curator_runs: '0',
        curator_requests: '0',
        curator_input_tokens: '0',
        curator_output_tokens: '0',
        curator_candidates_processed: '0',
        curator_candidates_deferred: '0',
        pending_queue_rows: '1',
        available_queue_rows: '1',
        oldest_queue_age_seconds: '60',
        next_queue_available_at: null,
        last_curator_run_at: null,
        next_curator_run_at: null,
        curator_claimed_until: '2026-05-23T10:30:00.000Z',
        last_curator_defer_reason: null
      }]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-05-23T10:20:00.000Z'));
    await expect(getCurationStatus('dff718f2-9d97-43b2-a3cc-a14099ed42c3')).resolves.toMatchObject({
      schedule: {
        next_run_at: '2026-05-23T10:30:00.000Z',
        eligible_now: false,
        defer_reason: 'curator run already claimed'
      }
    });
  });
});
