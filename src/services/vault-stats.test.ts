import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

import { getVaultStats } from './vault-stats';

function mockStatsRows() {
  queryMock.mockResolvedValueOnce({
    rows: [{
      plan_id: 'unlimited',
      period: '2026-06',
      ingest_events: '12',
      memory_adds: '3',
      searches: '7',
      limits: {
        memories_max: 1000,
        ingest_events_per_month: 100,
        memory_adds_per_month: 50,
        searches_per_month: 200
      }
    }]
  });
  queryMock.mockResolvedValueOnce({
    rows: [{
      active: '10',
      candidate: '2',
      needs_review: '1',
      contradicted: '0',
      superseded: '4',
      archived: '5'
    }]
  });
  queryMock.mockResolvedValueOnce({ rows: [{ count: '6' }] });
  queryMock.mockResolvedValueOnce({ rows: [{ last_run: '2026-06-01T00:00:00.000Z', arbitrations_this_week: '2' }] });
}

describe('getVaultStats', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  it('filters the stats query by account id when delegated account context is provided', async () => {
    mockStatsRows();

    await expect(getVaultStats(
      'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
    )).resolves.toMatchObject({
      vault_id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      plan: 'unlimited',
      memories: { active: 10 }
    });

    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('AND v.account_id = $3::uuid'),
      [
        'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        expect.any(String),
        'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      ]
    );
  });

  it('returns null without reading secondary stats tables when the vault is outside scope', async () => {
    queryMock.mockResolvedValueOnce({ rows: [] });

    await expect(getVaultStats(
      'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
    )).resolves.toBeNull();

    expect(queryMock).toHaveBeenCalledTimes(1);
  });
});
