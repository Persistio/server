import { describe, expect, it, vi } from 'vitest';

const configMock = vi.hoisted(() => ({
  OPENAI_API_KEY: 'test-openai-key',
  OPENAI_EMBEDDING_MODEL: 'text-embedding-3-small',
  TEI_BASE_URL: 'http://tei.test',
  TEI_EMBEDDING_MODEL: 'text-embeddings-inference',
  VERTEX_PROJECT_ID: 'persistio',
  VERTEX_LOCATION: 'europe-west2',
  VERTEX_EMBEDDING_MODEL: 'gemini-embedding-001',
  VERTEX_EMBEDDING_DIMENSIONS: 1536,
  INGEST_EMBEDDING_CONCURRENCY: 4
}));

vi.mock('../config', () => ({
  getStorageEmbeddingDimensions: () => 1536,
  getConfig: () => configMock
}));

import {
  OpenAIEmbedder,
  TeiEmbedder,
  VertexEmbedder,
  createOpenAIEmbeddingBatches,
  createTeiEmbeddingBatches,
  createVertexEmbeddingBatches
} from './embedder';

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

describe('TEI embedding provider', () => {
  it('splits requests at TEI default max_client_batch_size and preserves order', async () => {
    const batches = createTeiEmbeddingBatches(Array.from({ length: 65 }, (_, index) => `text ${index}`));

    expect(batches.map((batch) => batch.length)).toEqual([32, 32, 1]);
  });

  it('uses the TEI OpenAI-compatible embeddings endpoint and preserves order', async () => {
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({
        data: body.input.map((text: string, index: number) => ({
          index,
          embedding: [Number(text.slice(0, 2).trim()), 1]
        }))
      }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);

    const embedder = new TeiEmbedder();
    const result = await embedder.embedBatch([' 0 alpha', ' 1 beta']);

    expect(fetchMock).toHaveBeenCalledWith('http://tei.test/v1/embeddings', expect.objectContaining({
      method: 'POST'
    }));
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      model: 'text-embeddings-inference',
      input: [' 0 alpha', ' 1 beta'],
      encoding_format: 'float'
    });
    expect(result.map((embedding) => embedding[0])).toEqual([0, 1]);

    vi.unstubAllGlobals();
  });

  it('sends large TEI batches across multiple requests', async () => {
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({
        data: body.input.map((text: string, index: number) => ({
          index,
          embedding: [Number(text.slice(0, 2).trim()), 1]
        }))
      }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);

    const embedder = new TeiEmbedder();
    const inputs = Array.from({ length: 65 }, (_, index) =>
      String(index).padStart(2, ' ').padEnd(100, 'x')
    );
    const result = await embedder.embedBatch(inputs);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls.map((call) => JSON.parse(String(call[1]?.body)).input.length))
      .toEqual([32, 32, 1]);
    expect(result.map((embedding) => embedding[0])).toEqual(
      Array.from({ length: 65 }, (_, index) => index)
    );

    vi.unstubAllGlobals();
  });
});

describe('Vertex embedding provider', () => {
  it('uses conservative one-input batches for Gemini embeddings', () => {
    const batches = createVertexEmbeddingBatches(['alpha', 'beta', 'gamma']);

    expect(batches).toEqual([['alpha'], ['beta'], ['gamma']]);
  });

  it('uses the regional Vertex predict endpoint and preserves order', async () => {
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({
        predictions: body.instances.map((instance: { content: string }) => ({
          embeddings: {
            values: [Number(instance.content.slice(0, 2).trim()), 1]
          }
        }))
      }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);

    const embedder = new VertexEmbedder({
      getAccessToken: async () => 'test-token'
    });
    const result = await embedder.embedBatch([' 0 alpha', ' 1 beta']);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://europe-west2-aiplatform.googleapis.com/v1/projects/persistio/locations/europe-west2/publishers/google/models/gemini-embedding-001:predict'
    );
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({
      method: 'POST',
      headers: expect.objectContaining({
        Authorization: 'Bearer test-token',
        'Content-Type': 'application/json'
      })
    }));
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      instances: [{
        content: ' 0 alpha',
        task_type: 'RETRIEVAL_DOCUMENT'
      }],
      parameters: {
        outputDimensionality: 1536,
        autoTruncate: false
      }
    });
    expect(result.map((embedding) => embedding[0])).toEqual([0, 1]);

    vi.unstubAllGlobals();
  });

  it('uses the Vertex retrieval query task type for query embeddings', async () => {
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({
        predictions: body.instances.map(() => ({
          embeddings: {
            values: [1, 2]
          }
        }))
      }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);

    const embedder = new VertexEmbedder({
      getAccessToken: async () => 'test-token'
    });
    await embedder.embed('where is my deploy note?', { inputType: 'query' });

    const body = JSON.parse(String(fetchMock.mock.calls[0][1]?.body));
    expect(body.instances).toEqual([{
      content: 'where is my deploy note?',
      task_type: 'RETRIEVAL_QUERY'
    }]);
    expect(body.parameters).toEqual({
      outputDimensionality: 1536,
      autoTruncate: false
    });

    vi.unstubAllGlobals();
  });
});
