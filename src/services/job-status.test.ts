import type { PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import { completePersistentJobIfReady, failPersistentJob } from './job-status';

function createClient(rowsByCall: Array<{ rowCount: number; rows: unknown[] }>) {
  const query = vi.fn(async () => rowsByCall.shift() ?? { rowCount: 0, rows: [] });
  return {
    client: { query } as unknown as PoolClient,
    query
  };
}

describe('persistent job status transitions', () => {
  it('locks the parent job row before deciding a bulk job is complete', async () => {
    const { client, query } = createClient([
      { rowCount: 1, rows: [{ id: 'job-1' }] },
      { rowCount: 1, rows: [{ count: '0' }] },
      { rowCount: 1, rows: [{ count: '0' }] },
      { rowCount: 1, rows: [] }
    ]);

    await completePersistentJobIfReady(client, 'job-1');

    expect(query).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('FOR UPDATE'),
      ['job-1']
    );
    expect(query).toHaveBeenNthCalledWith(
      4,
      expect.stringContaining("status <> 'failed'"),
      ['job-1']
    );
  });

  it('does not complete while sibling queue rows remain', async () => {
    const { client, query } = createClient([
      { rowCount: 1, rows: [{ id: 'job-1' }] },
      { rowCount: 1, rows: [{ count: '1' }] },
      { rowCount: 1, rows: [{ count: '0' }] }
    ]);

    await completePersistentJobIfReady(client, 'job-1');

    expect(query).toHaveBeenCalledTimes(3);
    expect(query.mock.calls.some(([sql]) => String(sql).includes("SET status = 'completed'"))).toBe(false);
  });

  it('serializes failed transitions through the same parent job lock', async () => {
    const { client, query } = createClient([
      { rowCount: 1, rows: [{ id: 'job-1' }] },
      { rowCount: 1, rows: [] }
    ]);

    await failPersistentJob(client, 'job-1', 'segment failed');

    expect(query).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('FOR UPDATE'),
      ['job-1']
    );
    expect(query).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining("SET status = 'failed'"),
      ['job-1', 'segment failed']
    );
  });
});
