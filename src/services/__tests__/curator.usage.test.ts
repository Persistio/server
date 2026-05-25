import { beforeEach, describe, expect, it, vi } from 'vitest';

const { acquireAiBudgetMock, createMock, openAiMock, settleAiUsageMock } = vi.hoisted(() => ({
  acquireAiBudgetMock: vi.fn(),
  createMock: vi.fn(),
  openAiMock: vi.fn(),
  settleAiUsageMock: vi.fn()
}));

vi.hoisted(() => {
  process.env.CURATOR_BASE_URL = 'https://curator.example/v1';
  process.env.CURATOR_API_KEY = 'test-curator-key';
  process.env.CURATOR_MODEL = 'test-curator-model';
});

vi.mock('openai', () => ({
  default: openAiMock
}));

vi.mock('../usage', () => ({
  acquireAiBudget: acquireAiBudgetMock,
  settleAiUsage: settleAiUsageMock
}));

import { CuratorService, type CuratorMemory } from '../curator';

describe('CuratorService usage telemetry', () => {
  beforeEach(() => {
    acquireAiBudgetMock.mockReset();
    createMock.mockReset();
    openAiMock.mockReset();
    settleAiUsageMock.mockReset();
    openAiMock.mockImplementation(() => ({
      chat: {
        completions: {
          create: createMock
        }
      }
    }));
  });

  it('logs curator token/model usage and settles curation AI budget', async () => {
    createMock.mockResolvedValue({
      usage: {
        prompt_tokens: 120,
        completion_tokens: 30,
        total_tokens: 150
      },
      choices: [
        {
          message: {
            content: JSON.stringify({
              nodes_to_create: [],
              nodes_to_update: [],
              edges_to_create: [],
              nodes_to_archive: [],
              discarded_candidates: []
            })
          }
        }
      ]
    });
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => undefined);
    const service = new CuratorService();
    const candidates: CuratorMemory[] = [{
      id: 'candidate-1',
      subject: 'Persistio',
      data: 'Persistio should track curator model usage.',
      type: 'system_fact',
      scope: 'project',
      salience: 0.8,
      sensitivity: 'low',
      polarity: 'neutral',
      volatility: 'low',
      parent_id: null
    }];

    try {
      await service.curate(candidates, [], 'User asked for end to end model cost tracking.', 'vault-1');

      expect(acquireAiBudgetMock).toHaveBeenCalledWith('vault-1', 'curation', expect.any(Number));
      expect(settleAiUsageMock).toHaveBeenCalledWith('vault-1', 'curation', expect.any(Number), 150);
      expect(consoleSpy).toHaveBeenCalledWith(JSON.stringify({
        level: 30,
        msg: 'curator token usage',
        model: 'test-curator-model',
        model_role: 'curation',
        prompt_tokens: 120,
        completion_tokens: 30,
        total_tokens: 150,
        candidates_count: 1,
        active_memories_count: 0
      }));
    } finally {
      consoleSpy.mockRestore();
    }
  });

  it('bounds candidate, active memory, and conversation prompt sections to the input cap', async () => {
    createMock.mockResolvedValue({
      usage: undefined,
      choices: [
        {
          message: {
            content: JSON.stringify({
              nodes_to_create: [],
              nodes_to_update: [],
              edges_to_create: [],
              nodes_to_archive: [],
              discarded_candidates: []
            })
          }
        }
      ]
    });
    const service = new CuratorService();
    const longText = 'Important curator payload detail. '.repeat(200);
    const candidates: CuratorMemory[] = Array.from({ length: 4 }, (_, index) => ({
      id: `candidate-${index}`,
      subject: `Candidate ${index}`,
      data: longText,
      type: 'system_fact',
      scope: 'project',
      salience: 0.8,
      sensitivity: 'low',
      polarity: 'neutral',
      volatility: 'low',
      parent_id: null
    }));
    const activeMemories: CuratorMemory[] = Array.from({ length: 4 }, (_, index) => ({
      id: `active-${index}`,
      subject: `Candidate ${index}`,
      data: longText,
      type: 'system_fact',
      scope: 'project',
      salience: 0.7,
      sensitivity: 'low',
      polarity: 'neutral',
      volatility: 'low',
      parent_id: null
    }));

    await service.curate(candidates, activeMemories, 'Conversation detail. '.repeat(500), 'vault-1', {
      maxInputTokens: 500,
      maxOutputTokens: 25
    });

    const request = createMock.mock.calls[0]?.[0];
    const systemContent = request.messages[0].content as string;
    const userContent = request.messages[1].content as Array<{ type: 'text'; text: string }>;
    const userTextLength = userContent.reduce((sum, part) => sum + part.text.length, 0);

    expect(userTextLength).toBeLessThanOrEqual((500 * 4) - systemContent.length - 1000);
    expect(userContent[0].text).toContain('[truncated]');
    expect(userContent[1].text).toContain('[truncated]');
    expect(userContent[2].text).toContain('[truncated]');
    expect(request.max_tokens).toBe(25);
  });
});
