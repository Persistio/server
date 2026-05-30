import OpenAI from 'openai';
import pLimit from 'p-limit';

import { getConfig, STORAGE_EMBEDDING_DIMENSIONS } from '../config';
import { embeddingDurationHistogram } from '../metrics';
import { withSpan } from '../telemetry';

export interface Embedder {
  embed(text: string): Promise<number[]>;
  embedBatch(texts: string[]): Promise<number[][]>;
  dimensions: number;
}

export const OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT = 8192;

const OPENAI_EMBEDDING_MAX_BATCH_INPUTS = 2048;
const OPENAI_EMBEDDING_TARGET_TOKENS_PER_REQUEST = 250_000;
const ESTIMATED_BYTES_PER_TOKEN = 1;

function normalizeEmbedding(values: number[], dimensions: number): number[] {
  if (values.length === dimensions) {
    return values;
  }

  if (values.length > dimensions) {
    return values.slice(0, dimensions);
  }

  // Ollama commonly returns 768-dim embeddings; these are zero-padded to the
  // 1536-dim storage schema for compatibility, which works but can reduce
  // recall quality compared with storing native-dimension vectors.
  return values.concat(new Array(dimensions - values.length).fill(0));
}

export function estimateEmbeddingTokens(text: string): number {
  return Math.max(1, Math.ceil(Buffer.byteLength(text, 'utf8') / ESTIMATED_BYTES_PER_TOKEN));
}

export function createOpenAIEmbeddingBatches(texts: string[]): string[][] {
  const batches: string[][] = [];
  let currentBatch: string[] = [];
  let currentEstimatedTokens = 0;

  for (const text of texts) {
    const estimatedTokens = estimateEmbeddingTokens(text);
    if (estimatedTokens > OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT) {
      throw new Error(`Embedding input is too large: estimated ${estimatedTokens} tokens exceeds ${OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT}`);
    }

    const wouldExceedCount = currentBatch.length >= OPENAI_EMBEDDING_MAX_BATCH_INPUTS;
    const wouldExceedTokens = currentBatch.length > 0
      && currentEstimatedTokens + estimatedTokens > OPENAI_EMBEDDING_TARGET_TOKENS_PER_REQUEST;

    if (wouldExceedCount || wouldExceedTokens) {
      batches.push(currentBatch);
      currentBatch = [];
      currentEstimatedTokens = 0;
    }

    currentBatch.push(text);
    currentEstimatedTokens += estimatedTokens;
  }

  if (currentBatch.length) {
    batches.push(currentBatch);
  }

  return batches;
}

export class OpenAIEmbedder implements Embedder {
  public readonly dimensions = 1536;
  private readonly client: OpenAI;
  private readonly model: string;

  constructor(client?: OpenAI) {
    const config = getConfig();
    this.client = client ?? new OpenAI({ apiKey: config.OPENAI_API_KEY });
    this.model = config.OPENAI_EMBEDDING_MODEL;
  }

  async embed(text: string): Promise<number[]> {
    const [embedding] = await this.embedBatch([text]);
    return embedding ?? normalizeEmbedding([], STORAGE_EMBEDDING_DIMENSIONS);
  }

  async embedBatch(texts: string[]): Promise<number[][]> {
    if (!texts.length) {
      return [];
    }

    return withSpan('embedding.openai', {
      'embedding.model': this.model,
      'embedding.provider': 'openai',
      'embedding.input_count': texts.length
    }, async (span) => {
      const batches = createOpenAIEmbeddingBatches(texts);
      const results = new Array<number[]>(texts.length);
      let offset = 0;
      let totalDurationMs = 0;

      for (const batch of batches) {
        const start = performance.now();
        const response = await this.client.embeddings.create({
          model: this.model,
          input: batch
        });
        const durationMs = performance.now() - start;
        totalDurationMs += durationMs;
        embeddingDurationHistogram.record(durationMs, {
          provider: 'openai',
          model: this.model
        });

        const byIndex = new Map(response.data.map((item) => [item.index, item.embedding]));
        for (let index = 0; index < batch.length; index++) {
          results[offset + index] = normalizeEmbedding(
            byIndex.get(index) ?? response.data[index]?.embedding ?? [],
            STORAGE_EMBEDDING_DIMENSIONS
          );
        }
        offset += batch.length;
      }

      span.setAttribute('embedding.duration_ms', totalDurationMs);
      span.setAttribute('embedding.request_count', batches.length);
      return results;
    });
  }
}

export class OllamaEmbedder implements Embedder {
  public readonly dimensions = 768;
  private readonly baseUrl: string;
  private readonly model: string;

  constructor() {
    const config = getConfig();
    this.baseUrl = config.OLLAMA_BASE_URL.replace(/\/$/, '');
    this.model = config.OLLAMA_EMBEDDING_MODEL;
  }

  async embed(text: string): Promise<number[]> {
    return this.embedOne(text);
  }

  async embedBatch(texts: string[]): Promise<number[][]> {
    if (!texts.length) {
      return [];
    }

    const config = getConfig();
    const limit = pLimit(config.INGEST_EMBEDDING_CONCURRENCY);
    return Promise.all(texts.map((text) => limit(() => this.embedOne(text))));
  }

  private async embedOne(text: string): Promise<number[]> {
    return withSpan('embedding.ollama', {
      'embedding.model': this.model,
      'embedding.provider': 'ollama'
    }, async (span) => {
      const start = performance.now();
      const response = await fetch(`${this.baseUrl}/api/embeddings`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          model: this.model,
          prompt: text
        })
      });

      if (!response.ok) {
        throw new Error(`Ollama embedding request failed with ${response.status}`);
      }

      const payload = await response.json() as { embedding?: number[] };
      const durationMs = performance.now() - start;
      embeddingDurationHistogram.record(durationMs, {
        provider: 'ollama',
        model: this.model
      });
      span.setAttribute('embedding.duration_ms', durationMs);
      return normalizeEmbedding(payload.embedding ?? [], STORAGE_EMBEDDING_DIMENSIONS);
    });
  }
}

let cachedEmbedder: Embedder | undefined;

export function getEmbedder(): Embedder {
  if (cachedEmbedder) {
    return cachedEmbedder;
  }

  const config = getConfig();
  cachedEmbedder = config.EMBEDDER_PROVIDER === 'ollama'
    ? new OllamaEmbedder()
    : new OpenAIEmbedder();
  return cachedEmbedder;
}
