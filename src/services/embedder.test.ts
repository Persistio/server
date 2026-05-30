import { describe, expect, it, vi } from 'vitest';

vi.mock('../config', () => ({
  STORAGE_EMBEDDING_DIMENSIONS: 1536,
  getConfig: () => ({
    OPENAI_API_KEY: 'test-openai-key',
    OPENAI_EMBEDDING_MODEL: 'text-embedding-3-small'
  })
}));

import { OpenAIEmbedder, createOpenAIEmbeddingBatches } from './embedder';

describe('OpenAI embedding batching', () => {
  it('splits large bulk inputs below the provider total-token request limit', () => {
    const batches = createOpenAIEmbeddingBatches(
      Array.from({ length: 600 }, () => 'x'.repeat(1000))
    );

    expect(batches.length).toBeGreaterThan(1);
    for (const batch of batches) {
      expect(batch.reduce((total, text) => total + text.length, 0)).toBeLessThanOrEqual(250_000);
    }
  });

  it('rejects a single input above the per-input embedding limit', () => {
    expect(() => createOpenAIEmbeddingBatches(['x'.repeat(8193)])).toThrow(/exceeds 8192/);
  });

  it('sends split OpenAI requests and preserves original embedding order', async () => {
    const createMock = vi.fn(async ({ input }: { input: string[] }) => ({
      data: input.map((text, index) => ({
        index,
        embedding: [Number(text.slice(0, 2).trim()), 1]
      }))
    }));
    const embedder = new OpenAIEmbedder({
      embeddings: {
        create: createMock
      }
    } as never);
    const inputs = Array.from({ length: 33 }, (_, index) =>
      String(index).padStart(2, ' ').padEnd(8000, 'x')
    );

    const result = await embedder.embedBatch(inputs);

    expect(createMock).toHaveBeenCalledTimes(2);
    expect(createMock.mock.calls[0][0].input).toHaveLength(31);
    expect(createMock.mock.calls[1][0].input).toHaveLength(2);
    expect(result.map((embedding) => embedding[0])).toEqual(
      Array.from({ length: 33 }, (_, index) => index)
    );
  });
});
