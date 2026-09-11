import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock, recordMemoryCountDeltaMock } = vi.hoisted(() => ({
  queryMock: vi.fn(),
  recordMemoryCountDeltaMock: vi.fn()
}));

vi.mock('../config', () => ({
  getConfig: () => ({
    CONFIDENCE_DECAY_AUTO_ARCHIVE_SALIENCE_THRESHOLD: 0.2,
    CONFIDENCE_DECAY_INTERVAL_DAYS: 30,
    MEMORY_ARCHIVE_TTL_DAYS: 365
  })
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

vi.mock('./usage', () => ({
  recordMemoryCountDelta: recordMemoryCountDeltaMock
}));

import { archiveStaleMemories } from './staleness';

describe('archiveStaleMemories', () => {
  beforeEach(() => {
    queryMock.mockReset();
    recordMemoryCountDeltaMock.mockReset();
  });

  it('emits negative worker memory-count deltas for rows archived by staleness cleanup', async () => {
    queryMock
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ vault_id: 'vault-1', account_id: 'account-1', archived_count: '2' }]
      })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ vault_id: 'vault-2', account_id: 'account-2', archived_count: '3' }]
      })
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ vault_id: 'vault-3', account_id: 'account-3', archived_count: '1' }]
      });

    await archiveStaleMemories();

    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(/WITH archived AS \([\s\S]+memories\.valid_until < \(now\(\) AT TIME ZONE 'UTC'\)::date[\s\S]+GROUP BY vault_id, account_id/)
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(/WITH archived AS \([\s\S]+RETURNING memories\.vault_id::text AS vault_id, vaults\.account_id::text AS account_id[\s\S]+GROUP BY vault_id, account_id/),
      [365]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      4,
      expect.stringMatching(/WITH archived AS \([\s\S]+memories\.confidence <= 0[\s\S]+GROUP BY vault_id, account_id/),
      [0.2]
    );
    expect(recordMemoryCountDeltaMock).toHaveBeenNthCalledWith(1, 'vault-1', 'account-1', -2, 'extraction_worker');
    expect(recordMemoryCountDeltaMock).toHaveBeenNthCalledWith(2, 'vault-2', 'account-2', -3, 'extraction_worker');
    expect(recordMemoryCountDeltaMock).toHaveBeenNthCalledWith(3, 'vault-3', 'account-3', -1, 'extraction_worker');
  });

  it('starts inactivity at UTC activation and leaves future memories out of confidence cleanup', async () => {
    queryMock.mockResolvedValue({ rowCount: 0, rows: [] });

    await archiveStaleMemories();

    expect(queryMock.mock.calls[1][0]).toMatch(/GREATEST\(\s+memories\.valid_from::timestamp AT TIME ZONE 'UTC',\s+COALESCE\(memories\.last_recalled, memories\.updated_at, memories\.created_at\)/);
    expect(queryMock.mock.calls[2][0]).toMatch(/GREATEST\(\s+valid_from::timestamp AT TIME ZONE 'UTC',\s+COALESCE\(last_recalled, updated_at, created_at\)/);
    expect(queryMock.mock.calls[2][0]).toContain('confidence = GREATEST(confidence - 1, 0)');
    expect(queryMock.mock.calls[2][0]).toContain("status = CASE WHEN status = 'active' AND confidence <= 1 THEN 'needs_review' ELSE status END");
    expect(queryMock.mock.calls[3][0]).toContain("memories.valid_from <= (now() AT TIME ZONE 'UTC')::date");
  });
});
