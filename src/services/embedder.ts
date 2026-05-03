import OpenAI from 'openai';

import { getConfig, STORAGE_EMBEDDING_DIMENSIONS } from '../config';
import { embeddingDurationHistogram } from '../metrics';
import { withSpan } from '../telemetry';

export interface Embedder {
  embed(text: string): Promise<number[]>;
  dimensions: number;
}

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

export class OpenAIEmbedder implements Embedder {
  public readonly dimensions = 1536;
  private readonly client: OpenAI;
  private readonly model: string;

  constructor() {
    const config = getConfig();
    this.client = new OpenAI({ apiKey: config.OPENAI_API_KEY });
    this.model = config.OPENAI_EMBEDDING_MODEL;
  }

  async embed(text: string): Promise<number[]> {
    return withSpan('embedding.openai', {
      'embedding.model': this.model,
      'embedding.provider': 'openai'
    }, async (span) => {
      const start = performance.now();
      const response = await this.client.embeddings.create({
        model: this.model,
        input: text
      });
      const durationMs = performance.now() - start;
      embeddingDurationHistogram.record(durationMs, {
        provider: 'openai',
        model: this.model
      });
      span.setAttribute('embedding.duration_ms', durationMs);
      return normalizeEmbedding(response.data[0]?.embedding ?? [], STORAGE_EMBEDDING_DIMENSIONS);
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
