import OpenAI from 'openai';
import { GoogleAuth } from 'google-auth-library';
import pLimit from 'p-limit';
import type { PoolClient } from 'pg';

import { getConfig, getStorageEmbeddingDimensions } from '../config';
import { embeddingDurationHistogram } from '../metrics';
import { withSpan } from '../telemetry';
import type { CustomerMetricSource } from './customer-metrics';
import { recordModelUsage, type ModelUsageRole } from './usage';

export interface Embedder {
  embed(text: string, telemetry?: EmbedderTelemetry): Promise<number[]>;
  embedBatch(texts: string[], telemetry?: EmbedderTelemetry): Promise<number[][]>;
  dimensions: number;
}

export const OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT = 8192;

export interface EmbedderTelemetry {
  inputType?: 'document' | 'query';
  modelRole?: ModelUsageRole;
  source?: CustomerMetricSource;
  usageClient?: Pick<PoolClient, 'query'>;
  vaultId?: string;
}

const OPENAI_EMBEDDING_MAX_BATCH_INPUTS = 2048;
const OPENAI_EMBEDDING_TARGET_TOKENS_PER_REQUEST = 250_000;
const TEI_EMBEDDING_MAX_BATCH_INPUTS = 32;
const VERTEX_EMBEDDING_MAX_BATCH_INPUTS = 1;
const VERTEX_DOCUMENT_TASK_TYPE = 'RETRIEVAL_DOCUMENT';
const VERTEX_QUERY_TASK_TYPE = 'RETRIEVAL_QUERY';
const ESTIMATED_BYTES_PER_TOKEN = 1;

function normalizeEmbedding(values: number[], dimensions: number): number[] {
  if (values.length === dimensions) {
    return values;
  }

  if (values.length > dimensions) {
    return values.slice(0, dimensions);
  }

  // Provider dimensions can differ from the storage schema during comparisons;
  // normalize before writing to pgvector so inserts and similarity queries agree.
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

export function createTeiEmbeddingBatches(texts: string[]): string[][] {
  const batches: string[][] = [];

  for (const batch of createOpenAIEmbeddingBatches(texts)) {
    for (let index = 0; index < batch.length; index += TEI_EMBEDDING_MAX_BATCH_INPUTS) {
      batches.push(batch.slice(index, index + TEI_EMBEDDING_MAX_BATCH_INPUTS));
    }
  }

  return batches;
}

export function createVertexEmbeddingBatches(texts: string[]): string[][] {
  const batches: string[][] = [];
  for (const batch of createOpenAIEmbeddingBatches(texts)) {
    for (let index = 0; index < batch.length; index += VERTEX_EMBEDDING_MAX_BATCH_INPUTS) {
      batches.push(batch.slice(index, index + VERTEX_EMBEDDING_MAX_BATCH_INPUTS));
    }
  }
  return batches;
}

export class OpenAIEmbedder implements Embedder {
  private readonly client: OpenAI;
  private readonly model: string;

  get dimensions(): number {
    return getStorageEmbeddingDimensions();
  }

  constructor(client?: OpenAI) {
    const config = getConfig();
    this.client = client ?? new OpenAI({
      apiKey: config.OPENAI_API_KEY,
      baseURL: 'https://api.openai.com/v1'
    });
    this.model = config.OPENAI_EMBEDDING_MODEL;
  }

  async embed(text: string, telemetry?: EmbedderTelemetry): Promise<number[]> {
    const [embedding] = await this.embedBatch([text], telemetry);
    return embedding ?? normalizeEmbedding([], this.dimensions);
  }

  async embedBatch(texts: string[], telemetry?: EmbedderTelemetry): Promise<number[][]> {
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
            this.dimensions
          );
        }
        offset += batch.length;
      }

      await recordEmbeddingUsage({
        telemetry,
        provider: 'openai',
        model: this.model,
        requestCount: batches.length,
        texts
      });
      span.setAttribute('embedding.duration_ms', totalDurationMs);
      span.setAttribute('embedding.request_count', batches.length);
      return results;
    });
  }
}

interface AccessTokenProvider {
  getAccessToken(): Promise<string | null | undefined>;
}

export class VertexEmbedder implements Embedder {
  private readonly auth: AccessTokenProvider;
  private readonly projectId: string;
  private readonly location: string;
  private readonly model: string;
  private readonly outputDimensions: number;

  get dimensions(): number {
    return getStorageEmbeddingDimensions();
  }

  constructor(auth?: AccessTokenProvider) {
    const config = getConfig();
    this.auth = auth ?? new GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/cloud-platform']
    });
    this.projectId = config.VERTEX_PROJECT_ID;
    this.location = config.VERTEX_LOCATION;
    this.model = config.VERTEX_EMBEDDING_MODEL;
    this.outputDimensions = config.VERTEX_EMBEDDING_DIMENSIONS || this.dimensions;
  }

  async embed(text: string, telemetry?: EmbedderTelemetry): Promise<number[]> {
    const [embedding] = await this.embedBatch([text], telemetry);
    return embedding ?? normalizeEmbedding([], this.dimensions);
  }

  async embedBatch(texts: string[], telemetry?: EmbedderTelemetry): Promise<number[][]> {
    if (!texts.length) {
      return [];
    }

    return withSpan('embedding.vertex', {
      'embedding.model': this.model,
      'embedding.provider': 'vertex',
      'embedding.input_count': texts.length
    }, async (span) => {
      const config = getConfig();
      const batches = createVertexEmbeddingBatches(texts);
      const results = new Array<number[]>(texts.length);
      const limit = pLimit(config.INGEST_EMBEDDING_CONCURRENCY);
      let nextOffset = 0;
      const indexedBatches = batches.map((batch) => {
        const offset = nextOffset;
        nextOffset += batch.length;
        return { batch, offset };
      });
      let totalDurationMs = 0;

      await Promise.all(indexedBatches.map(({ batch, offset }) => limit(async () => {
        const start = performance.now();
        const embeddings = await this.requestEmbeddings(batch, telemetry?.inputType ?? 'document');
        const durationMs = performance.now() - start;
        totalDurationMs += durationMs;
        embeddingDurationHistogram.record(durationMs, {
          provider: 'vertex',
          model: this.model
        });

        for (let index = 0; index < batch.length; index++) {
          results[offset + index] = normalizeEmbedding(embeddings[index] ?? [], this.dimensions);
        }
      })));

      await recordEmbeddingUsage({
        telemetry,
        provider: 'vertex',
        model: this.model,
        requestCount: batches.length,
        texts
      });
      span.setAttribute('embedding.duration_ms', totalDurationMs);
      span.setAttribute('embedding.request_count', batches.length);
      return results;
    });
  }

  private async requestEmbeddings(texts: string[], inputType: NonNullable<EmbedderTelemetry['inputType']>): Promise<number[][]> {
    const accessToken = await this.auth.getAccessToken();
    if (!accessToken) {
      throw new Error('Could not acquire Google Cloud access token for Vertex embeddings');
    }

    const url = `https://${this.location}-aiplatform.googleapis.com/v1/projects/${this.projectId}/locations/${this.location}/publishers/google/models/${this.model}:predict`;
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        instances: texts.map((text) => ({
          content: text,
          task_type: inputType === 'query' ? VERTEX_QUERY_TASK_TYPE : VERTEX_DOCUMENT_TASK_TYPE
        })),
        parameters: {
          outputDimensionality: this.outputDimensions,
          autoTruncate: false
        }
      })
    });

    if (!response.ok) {
      const details = await response.text().catch(() => '');
      throw new Error(`Vertex embedding request failed with ${response.status}${details ? `: ${details}` : ''}`);
    }

    const payload = await response.json() as {
      predictions?: unknown[];
    };
    return (payload.predictions ?? []).map((prediction) => extractVertexEmbedding(prediction));
  }
}

export class OllamaEmbedder implements Embedder {
  private readonly baseUrl: string;
  private readonly model: string;

  get dimensions(): number {
    return getStorageEmbeddingDimensions();
  }

  constructor() {
    const config = getConfig();
    this.baseUrl = config.OLLAMA_BASE_URL.replace(/\/$/, '');
    this.model = config.OLLAMA_EMBEDDING_MODEL;
  }

  async embed(text: string, telemetry?: EmbedderTelemetry): Promise<number[]> {
    return this.embedOne(text, telemetry);
  }

  async embedBatch(texts: string[], telemetry?: EmbedderTelemetry): Promise<number[][]> {
    if (!texts.length) {
      return [];
    }

    const config = getConfig();
    const limit = pLimit(config.INGEST_EMBEDDING_CONCURRENCY);
    return Promise.all(texts.map((text) => limit(() => this.embedOne(text, telemetry))));
  }

  private async embedOne(text: string, telemetry?: EmbedderTelemetry): Promise<number[]> {
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
      await recordEmbeddingUsage({
        telemetry,
        provider: 'ollama',
        model: this.model,
        requestCount: 1,
        texts: [text]
      });
      span.setAttribute('embedding.duration_ms', durationMs);
      return normalizeEmbedding(payload.embedding ?? [], this.dimensions);
    });
  }
}

export class TeiEmbedder implements Embedder {
  private readonly baseUrl: string;
  private readonly model: string;

  get dimensions(): number {
    return getStorageEmbeddingDimensions();
  }

  constructor() {
    const config = getConfig();
    this.baseUrl = config.TEI_BASE_URL.replace(/\/$/, '');
    this.model = config.TEI_EMBEDDING_MODEL;
  }

  async embed(text: string, telemetry?: EmbedderTelemetry): Promise<number[]> {
    const [embedding] = await this.embedBatch([text], telemetry);
    return embedding ?? normalizeEmbedding([], this.dimensions);
  }

  async embedBatch(texts: string[], telemetry?: EmbedderTelemetry): Promise<number[][]> {
    if (!texts.length) {
      return [];
    }

    return withSpan('embedding.tei', {
      'embedding.model': this.model,
      'embedding.provider': 'tei',
      'embedding.input_count': texts.length
    }, async (span) => {
      const batches = createTeiEmbeddingBatches(texts);
      const results = new Array<number[]>(texts.length);
      let offset = 0;
      let totalDurationMs = 0;

      for (const batch of batches) {
        const start = performance.now();
        const response = await fetch(`${this.baseUrl}/v1/embeddings`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            model: this.model,
            input: batch,
            encoding_format: 'float'
          })
        });

        if (!response.ok) {
          const details = await response.text().catch(() => '');
          throw new Error(`TEI embedding request failed with ${response.status}${details ? `: ${details}` : ''}`);
        }

        const payload = await response.json() as {
          data?: Array<{ embedding?: number[]; index?: number }>;
        };
        const durationMs = performance.now() - start;
        totalDurationMs += durationMs;
        embeddingDurationHistogram.record(durationMs, {
          provider: 'tei',
          model: this.model
        });

        const data = payload.data ?? [];
        const byIndex = new Map(data.map((item, index) => [item.index ?? index, item.embedding ?? []]));
        for (let index = 0; index < batch.length; index++) {
          results[offset + index] = normalizeEmbedding(
            byIndex.get(index) ?? [],
            this.dimensions
          );
        }
        offset += batch.length;
      }

      await recordEmbeddingUsage({
        telemetry,
        provider: 'tei',
        model: this.model,
        requestCount: batches.length,
        texts
      });
      span.setAttribute('embedding.duration_ms', totalDurationMs);
      span.setAttribute('embedding.request_count', batches.length);
      return results;
    });
  }
}

let cachedEmbedder: Embedder | undefined;

export function getEmbedder(): Embedder {
  if (cachedEmbedder) {
    return cachedEmbedder;
  }

  const config = getConfig();
  if (config.EMBEDDER_PROVIDER === 'ollama') {
    cachedEmbedder = new OllamaEmbedder();
  } else if (config.EMBEDDER_PROVIDER === 'tei') {
    cachedEmbedder = new TeiEmbedder();
  } else if (config.EMBEDDER_PROVIDER === 'vertex') {
    cachedEmbedder = new VertexEmbedder();
  } else {
    cachedEmbedder = new OpenAIEmbedder();
  }
  return cachedEmbedder;
}

function extractVertexEmbedding(prediction: unknown): number[] {
  if (!prediction || typeof prediction !== 'object') {
    return [];
  }

  const record = prediction as {
    values?: unknown;
    embeddings?: {
      values?: unknown;
    };
  };
  const values = Array.isArray(record.embeddings?.values)
    ? record.embeddings.values
    : Array.isArray(record.values)
      ? record.values
      : [];

  return values
    .map((value) => typeof value === 'number' ? value : Number(value))
    .filter((value) => Number.isFinite(value));
}

async function recordEmbeddingUsage(input: {
  telemetry: EmbedderTelemetry | undefined;
  provider: string;
  model: string;
  requestCount: number;
  texts: string[];
}) {
  if (!input.telemetry?.vaultId) {
    return;
  }

  try {
    await recordModelUsage(
      {
        vaultId: input.telemetry.vaultId,
        provider: input.provider,
        model: input.model,
        modelRole: input.telemetry.modelRole ?? 'embedding',
        source: input.telemetry.source ?? 'api',
        requestCount: input.requestCount,
        embeddingCalls: input.texts.length,
        embeddingInputTokens: input.texts.reduce((sum, text) => sum + estimateEmbeddingTokens(text), 0),
        embeddingInputChars: input.texts.reduce((sum, text) => sum + Buffer.byteLength(text, 'utf8'), 0)
      },
      input.telemetry.usageClient
    );
  } catch (error) {
    console.warn(JSON.stringify({
      level: 40,
      msg: 'failed to record embedding usage',
      provider: input.provider,
      model: input.model,
      model_role: input.telemetry.modelRole ?? 'embedding',
      vault_id: input.telemetry.vaultId,
      err: error instanceof Error ? error.message : String(error)
    }));
  }
}
