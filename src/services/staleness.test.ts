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
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ vault_id: 'vault-2', account_id: 'account-2', archived_count: '1' }]
      });

    await archiveStaleMemories();

    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(/WITH archived AS \([\s\S]+RETURNING memories\.vault_id::text AS vault_id, vaults\.account_id::text AS account_id[\s\S]+GROUP BY vault_id, account_id/),
      [365]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      3,
      expect.stringMatching(/WITH archived AS \([\s\S]+memories\.confidence <= 0[\s\S]+GROUP BY vault_id, account_id/),
      [0.2]
    );
    expect(recordMemoryCountDeltaMock).toHaveBeenNthCalledWith(1, 'vault-1', 'account-1', -2, 'extraction_worker');
    expect(recordMemoryCountDeltaMock).toHaveBeenNthCalledWith(2, 'vault-2', 'account-2', -1, 'extraction_worker');
  });
});
