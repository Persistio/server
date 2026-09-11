import { beforeEach, describe, expect, it, vi } from 'vitest';

const { createMock, openAiMock } = vi.hoisted(() => ({
  createMock: vi.fn(),
  openAiMock: vi.fn()
}));

vi.mock('openai', () => ({
  default: openAiMock
}));

vi.mock('../usage', () => ({
  acquireAiBudget: vi.fn(),
  recordModelUsage: vi.fn(),
  settleAiUsage: vi.fn()
}));

import { ExtractorService, resolveExtractorRoleConfig } from '../extractor';
import { acquireAiBudget, recordModelUsage, settleAiUsage } from '../usage';

process.env.DATABASE_URL ??= 'postgres://localhost:5432/persistio_test';
process.env.ADMIN_API_KEY ??= 'test-admin-key';
process.env.EXTRACTOR_API_KEY ??= 'test-extractor-key';

describe('ExtractorService.arbitrateConflictsBatch', () => {
  beforeEach(() => {
    createMock.mockReset();
    openAiMock.mockReset();
    vi.mocked(acquireAiBudget).mockReset();
    vi.mocked(recordModelUsage).mockReset();
    vi.mocked(settleAiUsage).mockReset();
    openAiMock.mockImplementation(function OpenAIMock() {
      return {
        chat: {
          completions: {
            create: createMock
          }
        }
      };
    });
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
    ], 'vault-1');

    expect(arbitrateConflictSpy).toHaveBeenCalledWith('Old fact', 'New fact', 'vault-1');
    expect(result).toEqual(new Map([['pair-1', 'merge']]));
    expect(createMock).not.toHaveBeenCalled();
  });

  it('returns decisions in order for multi-pair responses', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [
        {
          finish_reason: 'stop',
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
    ], 'vault-1');

    expect(createMock).toHaveBeenCalledTimes(1);
    expect(createMock.mock.calls[0][0].model).toBe('gpt-4o-mini');
    expect(result).toEqual(new Map([
      ['pair-1', 'supersede_old'],
      ['pair-2', 'discard_new'],
      ['pair-3', 'merge']
    ]));
  });

  it('passes vaultId through multi-pair batch arbitration for quota attribution', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [{ finish_reason: 'stop', message: { content: '["merge","discard_new"]' } }]
    });
    const service = new ExtractorService();

    await service.arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'A', newFact: 'B' },
      { id: 'pair-2', existingFact: 'C', newFact: 'D' }
    ], 'vault-1');

    expect(createMock).toHaveBeenCalledTimes(1);
  });

  it('fails without decisions when the model returns malformed json', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [
        {
          finish_reason: 'stop',
          message: {
            content: 'not valid json'
          }
        }
      ]
    });
    const service = new ExtractorService();

    await expect(service.arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'A', newFact: 'B' },
      { id: 'pair-2', existingFact: 'C', newFact: 'D' }
    ])).rejects.toThrow('Invalid batch conflict arbitration JSON');
  });

  it.each([
    ['explanatory text', 'The answer is MERGE because they overlap.'],
    ['unknown text', 'KEEP_BOTH'],
    ['empty text', '']
  ])('rejects %s instead of interpreting a substring or defaulting', async (_label, content) => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [{ finish_reason: 'stop', message: { content } }]
    });

    await expect(new ExtractorService().arbitrateConflict('A', 'B'))
      .rejects.toThrow('Invalid conflict arbitration decision');
  });

  it.each([
    ['omitted', '["merge"]'],
    ['extra', '["merge","discard_new","needs_review"]'],
    ['unknown', '["merge","keep_both"]']
  ])('rejects %s batch decisions without returning a mutable fallback', async (_label, content) => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [{ finish_reason: 'stop', message: { content } }]
    });

    await expect(new ExtractorService().arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'A', newFact: 'B' },
      { id: 'pair-2', existingFact: 'C', newFact: 'D' }
    ])).rejects.toThrow('exactly one valid decision per pair');
  });

  it('rejects truncated arbitration before parsing any decision', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [{ finish_reason: 'length', message: { content: 'MERGE' } }]
    });

    await expect(new ExtractorService().arbitrateConflict('A', 'B'))
      .rejects.toThrow('did not complete cleanly');
  });

  it('creates separate extraction and escalation role clients with legacy fallbacks', async () => {
    const service = new ExtractorService();

    expect(openAiMock).toHaveBeenCalledTimes(2);
    expect(openAiMock).toHaveBeenNthCalledWith(1, {
      apiKey: 'test-extractor-key',
      baseURL: 'https://api.openai.com/v1'
    });
    expect(openAiMock).toHaveBeenNthCalledWith(2, {
      apiKey: 'test-extractor-key',
      baseURL: 'https://api.openai.com/v1'
    });

    expect(service).toBeInstanceOf(ExtractorService);
  });

  it('routes subject arbitration through the extraction role', async () => {
    createMock.mockResolvedValue({
      usage: {
        prompt_tokens: 20,
        completion_tokens: 1,
        total_tokens: 21
      },
      choices: [{ finish_reason: 'stop', message: { content: 'USE_EXISTING' } }]
    });
    const service = new ExtractorService();

    const result = await service.arbitrateSubject('Persistio', 'Persistio API', 'vault-1');

    expect(result).toBe('use_existing');
    expect(createMock).toHaveBeenCalledTimes(1);
    expect(createMock.mock.calls[0][0].model).toBe('gpt-4o-mini');
    expect(acquireAiBudget).toHaveBeenCalledWith('vault-1', 'extraction', expect.any(Number));
    expect(settleAiUsage).toHaveBeenCalledWith('vault-1', 'extraction', expect.any(Number), 21);
    expect(recordModelUsage).toHaveBeenCalledWith(expect.objectContaining({
      vaultId: 'vault-1',
      provider: 'openai',
      model: 'gpt-4o-mini',
      modelRole: 'extraction',
      source: 'extraction_worker',
      requestCount: 1,
      promptTokens: 20,
      completionTokens: 1,
      totalTokens: 21
    }));
  });

  it('rejects non-enum subject arbitration text', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [{ finish_reason: 'stop', message: { content: 'Probably USE_EXISTING' } }]
    });

    await expect(new ExtractorService().arbitrateSubject('Persistio', 'Persistio API'))
      .rejects.toThrow('Invalid subject arbitration decision');
  });
});

describe('resolveExtractorRoleConfig', () => {
  const legacyConfig = {
    EXTRACTOR_BASE_URL: 'https://legacy.example/v1',
    EXTRACTOR_API_KEY: 'legacy-key',
    EXTRACTOR_MODEL: 'legacy-model',
    EXTRACTION_BASE_URL: '',
    EXTRACTION_API_KEY: '',
    EXTRACTION_MODEL: '',
    ESCALATION_BASE_URL: '',
    ESCALATION_API_KEY: '',
    ESCALATION_MODEL: ''
  };

  it('falls back to legacy EXTRACTOR settings for both roles', () => {
    expect(resolveExtractorRoleConfig(legacyConfig)).toEqual({
      extraction: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      },
      escalation: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      }
    });
  });

  it('uses role-specific overrides when configured', () => {
    expect(resolveExtractorRoleConfig({
      ...legacyConfig,
      EXTRACTION_BASE_URL: 'https://flash.example/v1',
      EXTRACTION_API_KEY: 'flash-key',
      EXTRACTION_MODEL: 'gemini-2.5-flash',
      ESCALATION_BASE_URL: 'https://sonnet.example/v1',
      ESCALATION_API_KEY: 'sonnet-key',
      ESCALATION_MODEL: 'claude-sonnet-4-5'
    })).toEqual({
      extraction: {
        baseURL: 'https://flash.example/v1',
        apiKey: 'flash-key',
        model: 'gemini-2.5-flash'
      },
      escalation: {
        baseURL: 'https://sonnet.example/v1',
        apiKey: 'sonnet-key',
        model: 'claude-sonnet-4-5'
      }
    });
  });

  it('ignores incomplete role overrides so provider keys and endpoints are not mixed', () => {
    expect(resolveExtractorRoleConfig({
      ...legacyConfig,
      EXTRACTION_MODEL: 'gemini-2.5-flash'
    })).toEqual({
      extraction: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      },
      escalation: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model'
      }
    });
  });
});
