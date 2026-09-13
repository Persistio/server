import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock, recordMemoryCountDeltaMock, config } = vi.hoisted(() => ({
  queryMock: vi.fn(), recordMemoryCountDeltaMock: vi.fn(), config: { MEMORY_ARCHIVE_TTL_DAYS: 0 }
}));
vi.mock('../config', () => ({ getConfig: () => config }));
vi.mock('../db/client', () => ({ query: queryMock, withTransaction: (fn:Function) => fn({query:queryMock}) }));
vi.mock('./usage', () => ({ recordMemoryCountDelta: recordMemoryCountDeltaMock }));
import { archiveStaleMemories } from './staleness';

describe('opt-in inactivity retention', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    config.MEMORY_ARCHIVE_TTL_DAYS = 0;
    queryMock.mockResolvedValue({ rows: [] });
  });

  it('performs no ageing or confidence mutations by default', async () => {
    await archiveStaleMemories();
    expect(queryMock).not.toHaveBeenCalled();
    expect(recordMemoryCountDeltaMock).not.toHaveBeenCalled();
  });

  it('archives only under explicit TTL, without treating valid_until as deletion', async () => {
    config.MEMORY_ARCHIVE_TTL_DAYS = 365;
    queryMock.mockResolvedValueOnce({rows:[{vault_id:'v1'}]})
      .mockResolvedValueOnce({rows:[{account_id:'a1'}]})
      .mockResolvedValueOnce({rows:[],rowCount:2});
    await archiveStaleMemories();
    expect(queryMock).toHaveBeenCalledTimes(3);
    expect(queryMock.mock.calls[1][0]).toContain('FOR NO KEY UPDATE');
    const sql = queryMock.mock.calls[2][0];
    expect(sql).toContain('GREATEST(');
    expect(sql).toContain('last_recalled, updated_at, created_at');
    expect(sql).not.toMatch(/valid_until|confidence|needs_review|last_decayed_at/);
    expect(recordMemoryCountDeltaMock).toHaveBeenCalledExactlyOnceWith('v1', 'a1', -2, 'extraction_worker');
  });
});
