import { beforeEach, describe, expect, it, vi } from 'vitest';

const { createMock, openAiMock } = vi.hoisted(() => ({ createMock: vi.fn(), openAiMock: vi.fn() }));
vi.mock('openai', () => ({ default: openAiMock }));
vi.mock('../usage', () => ({ acquireAiBudget: vi.fn(), recordModelUsage: vi.fn(), settleAiUsage: vi.fn() }));

import { ExtractorService, type ConflictArbitrationContext } from '../extractor';

const context: ConflictArbitrationContext = {
  // B may be visited first during backfill despite describing an earlier fact.
  existing: { sourceTimestamp: '2026-09-08T10:00:00.000Z', validFrom: '2026-09-08', validUntil: null, createdAt: '2026-09-08T11:00:00.000Z' },
  incoming: { sourceTimestamp: '2026-08-01T10:00:00.000Z', validFrom: null, validUntil: '2026-09-09', createdAt: '2026-09-09T10:00:00.000Z' }
};

describe('contradiction arbitration context', () => {
  beforeEach(() => {
    createMock.mockReset();
    createMock.mockResolvedValue({ choices: [{ finish_reason: 'stop', message: { content: 'KEEP_BOTH' } }] });
    openAiMock.mockImplementation(function OpenAIMock() {
      return { chat: { completions: { create: createMock } } };
    });
  });

  it('does not treat chronology or memory text as retained source authority',async()=>{
    expect(await new ExtractorService().arbitrateConflict('Lives in Paris.','Lives in London.','vault-1',context)).toEqual({state:'incomplete'});
    expect(createMock).not.toHaveBeenCalled();
  });
  it.each(['SUPERSEDE_OLD','DISCARD_NEW','MERGE','KEEP_BOTH'])('does not dispatch to obtain an unbound legacy %s verdict',async content=>{
    createMock.mockResolvedValue({choices:[{finish_reason:'stop',message:{content}}]});
    expect(await new ExtractorService().arbitrateConflict('A','B','vault-1',context)).toEqual({state:'incomplete'});
    expect(createMock).not.toHaveBeenCalled();
  });
  it('does not manufacture keep_both when source metadata is absent',async()=>{
    expect(await new ExtractorService().arbitrateConflict('A','B','vault-1')).toEqual({state:'incomplete'});
    expect(createMock).not.toHaveBeenCalled();
  });
});
