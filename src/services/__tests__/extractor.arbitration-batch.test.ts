import { beforeEach, describe, expect, it, vi } from 'vitest';

const { createMock, openAiMock } = vi.hoisted(() => ({
  createMock: vi.fn(),
  openAiMock: vi.fn()
}));

vi.mock('openai', () => ({
  default: openAiMock
}));

import { ExtractorService, resolveExtractorRoleConfig, shouldApplyGeminiQuota } from '../extractor';

process.env.DATABASE_URL ??= 'postgres://localhost:5432/persistio_test';
process.env.ADMIN_API_KEY ??= 'test-admin-key';
process.env.EXTRACTOR_API_KEY ??= 'test-extractor-key';

describe('ExtractorService.arbitrateConflictsBatch', () => {
  beforeEach(() => {
    createMock.mockReset();
    openAiMock.mockReset();
    openAiMock.mockImplementation(() => ({
      chat: {
        completions: {
          create: createMock
        }
      }
    }));
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
      choices: [{ message: { content: '["merge","discard_new"]' } }]
    });
    const service = new ExtractorService();

    await service.arbitrateConflictsBatch([
      { id: 'pair-1', existingFact: 'A', newFact: 'B' },
      { id: 'pair-2', existingFact: 'C', newFact: 'D' }
    ], 'vault-1');

    expect(createMock).toHaveBeenCalledTimes(1);
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
        model: 'legacy-model',
        usesGeminiQuota: false
      },
      escalation: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model',
        usesGeminiQuota: false
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
        model: 'gemini-2.5-flash',
        usesGeminiQuota: true
      },
      escalation: {
        baseURL: 'https://sonnet.example/v1',
        apiKey: 'sonnet-key',
        model: 'claude-sonnet-4-5',
        usesGeminiQuota: false
      }
    });
  });

  it('supports partial role overrides by falling back field-by-field', () => {
    expect(resolveExtractorRoleConfig({
      ...legacyConfig,
      EXTRACTION_MODEL: 'gemini-2.5-flash'
    })).toEqual({
      extraction: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'gemini-2.5-flash',
        usesGeminiQuota: true
      },
      escalation: {
        baseURL: 'https://legacy.example/v1',
        apiKey: 'legacy-key',
        model: 'legacy-model',
        usesGeminiQuota: false
      }
    });
  });
});

describe('shouldApplyGeminiQuota', () => {
  it('applies Gemini quota for Gemini models or Google AI endpoints', () => {
    expect(shouldApplyGeminiQuota({
      baseURL: 'https://api.openai.com/v1',
      model: 'gemini-2.5-flash'
    })).toBe(true);
    expect(shouldApplyGeminiQuota({
      baseURL: 'https://generativelanguage.googleapis.com/v1beta/openai',
      model: 'custom-model'
    })).toBe(true);
    expect(shouldApplyGeminiQuota({
      baseURL: 'https://us-central1-aiplatform.googleapis.com/v1/projects/example/locations/us-central1/endpoints/openapi',
      model: 'custom-model'
    })).toBe(true);
  });

  it('does not apply Gemini quota to non-Gemini escalation providers', () => {
    expect(shouldApplyGeminiQuota({
      baseURL: 'https://api.anthropic.com/v1',
      model: 'claude-sonnet-4-5'
    })).toBe(false);
    expect(shouldApplyGeminiQuota({
      baseURL: 'https://api.openai.com/v1',
      model: 'gpt-4o-mini'
    })).toBe(false);
  });
});
