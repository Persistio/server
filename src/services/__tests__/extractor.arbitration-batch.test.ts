import { beforeEach, describe, expect, it, vi } from 'vitest';

const { createMock } = vi.hoisted(() => ({
  createMock: vi.fn()
}));

vi.mock('openai', () => ({
  default: vi.fn().mockImplementation(() => ({
    chat: {
      completions: {
        create: createMock
      }
    }
  }))
}));

import { ExtractorService } from '../extractor';

process.env.DATABASE_URL ??= 'postgres://localhost:5432/persistio_test';
process.env.ADMIN_API_KEY ??= 'test-admin-key';
process.env.EXTRACTOR_API_KEY ??= 'test-extractor-key';

describe('ExtractorService.arbitrateConflictsBatch', () => {
  beforeEach(() => {
    createMock.mockReset();
  });

  it('returns an empty map for empty input', async () => {
    const service = new ExtractorService();

    const result = await service.arbitrateConflictsBatch([]);

    expect(result).toEqual(new Map());
    expect(createMock).not.toHaveBeenCalled();
  });

  it('delegates single-pair input to arbitrateConflict', async () => {
    const service = new ExtractorService();
    const arbitrateConflictSpy = vi.spyOn(service, 'arbitrateConflict').mockResolvedValue('merge');

    const result = await service.arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'Old fact', newFact: 'New fact' }
    ]);

    expect(arbitrateConflictSpy).toHaveBeenCalledWith('Old fact', 'New fact');
    expect(result).toEqual(new Map([['pair-1', 'merge']]));
    expect(createMock).not.toHaveBeenCalled();
  });

  it('returns decisions in order for multi-pair responses', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [
        {
          message: {
            content: '["supersede_old","discard_new","merge"]'
          }
        }
      ]
    });
    const service = new ExtractorService();

    const result = await service.arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'A', newFact: 'B' },
      { id: 'pair-2', existingFact: 'C', newFact: 'D' },
      { id: 'pair-3', existingFact: 'E', newFact: 'F' }
    ]);

    expect(createMock).toHaveBeenCalledTimes(1);
    expect(result).toEqual(new Map([
      ['pair-1', 'supersede_old'],
      ['pair-2', 'discard_new'],
      ['pair-3', 'merge']
    ]));
  });

  it('falls back to needs_review when the model returns malformed json', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [
        {
          message: {
            content: 'not valid json'
          }
        }
      ]
    });
    const service = new ExtractorService();

    const result = await service.arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'A', newFact: 'B' },
      { id: 'pair-2', existingFact: 'C', newFact: 'D' }
    ]);

    expect(result).toEqual(new Map([
      ['pair-1', 'needs_review'],
      ['pair-2', 'needs_review']
    ]));
  });
});
