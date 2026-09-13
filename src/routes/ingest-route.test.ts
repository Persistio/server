import crypto from 'node:crypto';
import { ingestPayloadHash } from '../services/ingest-replay';
import Fastify, { type FastifyReply, type FastifyRequest } from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { configMock, embedBatchMock, queryMock, rawChunkDeleteMock, rawChunkPutMock, refundApiQuotaReservationMock, reserveApiQuotaMock, checkQuotaMock } = vi.hoisted(() => ({
  configMock: {
    EMBEDDER_PROVIDER: 'openai',
    MAX_INGEST_CHUNKS: 100,
    INGEST_RATE_LIMIT_RPM: 60,
    INGEST_CHUNK_MAX_CHARS: 8000,
    BULK_INGEST_MAX_CHUNKS: 2048,
    BULK_INGEST_BODY_LIMIT_BYTES: 2 * 1024 * 1024,
    SEGMENTATION_THRESHOLD: 0.75
  },
  embedBatchMock: vi.fn(),
  queryMock: vi.fn(),
  rawChunkDeleteMock: vi.fn(),
  rawChunkPutMock: vi.fn(),
  refundApiQuotaReservationMock: vi.fn(),
  reserveApiQuotaMock: vi.fn(),
  checkQuotaMock: vi.fn()
}));

const quotaReservation = {
  field: 'ingest_events',
  period: '2026-05',
  snapshot: {
    limit: null,
    remaining: null,
    resetAtEpochSeconds: null,
    retryAfterSeconds: null
  },
  vaultId: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070'
};

vi.mock('../config', () => ({
  getConfig: () => configMock
}));

vi.mock('../db/client', () => ({
  query: queryMock,
  withTransaction: async (callback: (client: { query: typeof queryMock }) => Promise<unknown>) => callback({ query: queryMock })
}));

vi.mock('../middleware/auth', () => ({
  requireVaultWriteAuth: async (request: FastifyRequest, _reply: FastifyReply) => {
    request.vault = {
      id: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      name: 'Premium Eval',
      purpose: null,
      settings: {},
      plan_id: 'unlimited',
      status: 'active',
      encrypted_dek: null,
      vault_encryption_enabled: false
    };
  }
}));

vi.mock('../services/crypto', () => ({
  prepareVaultCrypto: async () => ({encrypt: (_vault: unknown, value: string) => value,assertCurrent:async()=>{}})
}));

vi.mock('../services/embedder', () => ({
  OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT: 8192,
  estimateEmbeddingTokens: (text: string) => Math.max(1, Buffer.byteLength(text, 'utf8')),
  getEmbedder: () => ({
    embedBatch: embedBatchMock
  })
}));

vi.mock('../services/raw-chunk-storage', () => ({
  createRawChunkBlobKey: (vaultId: string, sessionId: string, chunkId: string) => `vaults/${vaultId}/sessions/${sessionId}/chunks/${chunkId}.txt`,
  getRawChunkStorage: () => ({
    store: 'local',
    put: rawChunkPutMock,
    delete: rawChunkDeleteMock
  })
}));

vi.mock('../services/usage', () => ({
  applyRateLimitHeaders: vi.fn(),
  refundApiQuotaReservation: refundApiQuotaReservationMock,
  reserveApiQuota: reserveApiQuotaMock,
  reserveApiQuotaInTransaction: reserveApiQuotaMock,
  recordCommittedApiQuotaReservation: vi.fn(),
  checkQuota: checkQuotaMock,
  consumeNormalIngestRateLimit: vi.fn(() => ({
    limit: 60,
    remaining: 59,
    resetAtEpochSeconds: 1,
    retryAfterSeconds: null
  })),
  isPremiumPlan: (planId: string) => planId === 'unlimited'
}));

// Route orchestration is isolated from the lifecycle's SQL protocol here.
// Its fencing/late-write behavior is exercised in lifecycle and PostgreSQL tests.
vi.mock('../services/raw-chunk-write-lifecycle', () => ({
  registerRawChunkWrites: vi.fn(),
  beginRawChunkUploads: vi.fn(),
  confirmRawChunkUpload: vi.fn(),
  lockCompletedRawChunkWrites: vi.fn(),
  cleanupRawChunkWrite: async (storage: { delete: (key: string) => Promise<void> }, selector: { blobKey: string }) => {
    const references = await queryMock('SELECT DISTINCT blob_key FROM raw_chunks', []);
    if (references.rows.some((row: { blob_key: string }) => row.blob_key === selector.blobKey)) return 'committed';
    try { await storage.delete(selector.blobKey); return 'deleted'; } catch { return 'failed'; }
  }
}));

vi.mock('../metrics', () => ({
  ingestChunksCounter: {
    add: vi.fn()
  }
}));

import { registerIngestRoutes } from './ingest';
import { sourceEventKey } from '../services/transport-provenance';
import { applyRateLimitHeaders, recordCommittedApiQuotaReservation } from '../services/usage';
import { ingestChunksCounter } from '../metrics';

function configurePersistence(options: {
  chunks: Array<{ timestamp: string }>;
  insertedIndexes?: number[];
  existingRows?: Array<{
    id: string;
    created_at: string;
    source_event_key: string;
    source_event_payload_sha256: string | null;
  }>;
  failRawInsert?: Error;
  referencedOnFailure?: boolean;
}) {
  const insertedIndexes = options.insertedIndexes ?? options.chunks.map((_chunk, index) => index);
  const referencedBlobKeys = new Set<string>();
  queryMock.mockImplementation(async (sql: string, parameters?: unknown[]) => {
    const text = String(sql);
    const params = parameters ?? [];
    if (text.includes('INSERT INTO raw_chunk_blob_write_intents')) {
      return { rowCount: (params[2] as string[]).length, rows: [] };
    }
    if (text.includes('FROM raw_chunk_blob_write_intents') && text.includes('FOR UPDATE')) {
      const keys = params[1] as string[];
      return { rowCount: keys.length, rows: keys.map((blob_key) => ({ blob_key })) };
    }
    if (text.includes('INSERT INTO jobs')) return { rowCount: 1, rows: [{ id: params[0] }] };
    if (text.includes('WITH input AS')) {
      const ids = params[2] as string[];
      const blobKeys = params[5] as string[];
      const timestamps = params[8] as string[];
      if (options.failRawInsert) {
        if (options.referencedOnFailure) blobKeys.forEach((key) => referencedBlobKeys.add(key));
        throw options.failRawInsert;
      }
      insertedIndexes.forEach((index) => referencedBlobKeys.add(blobKeys[index]));
      return {
        rowCount: insertedIndexes.length,
        rows: insertedIndexes.map((index) => ({ id: ids[index], created_at: timestamps[index], input_index: index }))
      };
    }
    if (text.includes('SELECT id,created_at,source_event_key')) {
      return { rowCount: options.existingRows?.length ?? 0, rows: options.existingRows ?? [] };
    }
    if (text.includes('SELECT DISTINCT blob_key')) {
      return { rowCount: referencedBlobKeys.size, rows: [...referencedBlobKeys].map((blob_key) => ({ blob_key })) };
    }
    return { rowCount: 1, rows: [] };
  });
}

describe('bulk ingest route body limit', () => {
  it.each(['/v1/ingest', '/v1/ingest/bulk'])('keeps accepted outcomes when post-commit metrics or headers fail at %s', async url => {
    const app = Fastify();
    await registerIngestRoutes(app);
    const chunks = [{ role: 'user', content: 'accepted even if telemetry fails', timestamp: '2026-05-12T16:00:00.000Z' }];
    configurePersistence({ chunks });
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    vi.mocked(applyRateLimitHeaders).mockImplementationOnce(() => { throw new Error('header failed'); });
    vi.mocked(recordCommittedApiQuotaReservation).mockImplementationOnce(() => { throw new Error('metric failed'); });
    vi.mocked(ingestChunksCounter.add).mockImplementation(() => { throw new Error('counter failed'); });
    try {
      const response = await app.inject({ method: 'POST', url, payload: { session_id: 'telemetry', chunks } });
      expect(response.statusCode, response.body).toBe(202);
      expect(response.json()).toMatchObject({ accepted: 1, inserted: 1, replayed: 0 });
      expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    } finally { vi.mocked(ingestChunksCounter.add).mockReset(); await app.close(); }
  });
  it.each(['/v1/ingest', '/v1/ingest/bulk'])('bounds chunk arrays before per-item validation at %s', async url => {
    const app = Fastify();
    await registerIngestRoutes(app);
    try {
      const count = url.endsWith('/bulk') ? configMock.BULK_INGEST_MAX_CHUNKS : configMock.MAX_INGEST_CHUNKS;
      const response = await app.inject({ method: 'POST', url, payload: { session_id: 'bounded', chunks: Array(count + 1).fill(null) } });
      expect(response.statusCode).toBe(413);
      expect(queryMock).not.toHaveBeenCalled();
      expect(reserveApiQuotaMock).not.toHaveBeenCalled();
    } finally { await app.close(); }
  });
  it('rejects a 24 MiB identity at the HTTP boundary without downstream side effects', async () => {
    const previousLimit = configMock.BULK_INGEST_BODY_LIMIT_BYTES;
    configMock.BULK_INGEST_BODY_LIMIT_BYTES = 25 * 1024 * 1024;
    const app = Fastify();
    const extraction = vi.fn();
    await registerIngestRoutes(app, extraction);
    try {
      const response = await app.inject({ method: 'POST', url: '/v1/ingest/bulk', payload: {
        session_id: 'bounded-validation', chunks: [{ role: 'user', content: 'payload', timestamp: '2026-06-01T00:00:00Z',
          source_event: { namespace: 'test', id: 'x'.repeat(24 * 1024 * 1024) } }]
      } });
      expect(response.statusCode).toBe(400);
      for (const effect of [reserveApiQuotaMock, embedBatchMock, queryMock, rawChunkPutMock, extraction]) expect(effect).not.toHaveBeenCalled();
    } finally { await app.close(); configMock.BULK_INGEST_BODY_LIMIT_BYTES = previousLimit; }
  });
  it.each(['/v1/ingest', '/v1/ingest/bulk'])('rejects invalid session identities without side effects at %s', async (url) => {
    const app = Fastify();
    const extraction = vi.fn();
    await registerIngestRoutes(app, extraction);
    try {
      for (const session_id of ['', ' ', '\nsession', 'session\t', 'bad\u0000id', 'bad\u007fid', 's'.repeat(513)]) {
        const response = await app.inject({ method: 'POST', url, payload: {
          session_id, chunks: [{ role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00Z' }]
        } });
        expect(response.statusCode).toBe(400);
      }
      for (const effect of [reserveApiQuotaMock, embedBatchMock, queryMock, rawChunkPutMock, extraction]) {
        expect(effect).not.toHaveBeenCalled();
      }
    } finally { await app.close(); }
  });
  beforeEach(() => {
    configMock.EMBEDDER_PROVIDER = 'openai';
    embedBatchMock.mockReset();
    queryMock.mockReset();
    queryMock.mockResolvedValue({ rowCount: 1, rows: [] });
    rawChunkDeleteMock.mockReset();
    rawChunkPutMock.mockReset();
    refundApiQuotaReservationMock.mockReset();
    reserveApiQuotaMock.mockReset();
    checkQuotaMock.mockReset();
    rawChunkPutMock.mockImplementation(async (key: string) => ({ blobStore: 'local', blobKey: key }));
    reserveApiQuotaMock.mockResolvedValue(quotaReservation);
    checkQuotaMock.mockResolvedValue(quotaReservation.snapshot);
  });

  it('accepts bulk payloads larger than Fastify default 1 MiB limit', async () => {
    const app = Fastify();
    const triggerExtraction = vi.fn();
    await registerIngestRoutes(app, triggerExtraction);

    const chunks = Array.from({ length: 160 }, (_, index) => ({
      role: index % 2 === 0 ? 'user' : 'assistant',
      content: String(index).padEnd(7000, index % 2 === 0 ? 'a' : 'b'),
      timestamp: new Date(Date.UTC(2026, 4, 12, 16, 0, index)).toISOString()
    }));
    embedBatchMock.mockResolvedValueOnce(chunks.map(() => [1, 0]));
    configurePersistence({ chunks });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks
      }
    });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toMatchObject({ accepted: 160 });
    expect(reserveApiQuotaMock).toHaveBeenCalledWith(
      expect.objectContaining({ query: expect.any(Function) }),
      '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      'ingest_events',
      'api'
    );
    expect(checkQuotaMock.mock.invocationCallOrder[0]).toBeLessThan(embedBatchMock.mock.invocationCallOrder[0]);
    expect(reserveApiQuotaMock.mock.invocationCallOrder[0]).toBeGreaterThan(embedBatchMock.mock.invocationCallOrder[0]);
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(rawChunkPutMock).toHaveBeenCalledTimes(chunks.length);
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('WITH input AS'))).toBe(true);
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('ORDER BY input.input_ordinal'))).toBe(true);
    expect(queryMock.mock.calls.some(([sql, parameters]) =>
      String(sql).includes('INSERT INTO segments') && Array.isArray(parameters) && parameters.at(-1) === 'backfill'
    )).toBe(true);
    expect(triggerExtraction).toHaveBeenCalledWith(expect.any(String), '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070');

    await app.close();
  });

  it('rejects chunks that exceed the configured per-chunk content contract before embedding', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [
          { role: 'user', content: 'a'.repeat(8001), timestamp: '2026-05-12T16:00:00.000Z' }
        ]
      }
    });

    expect(response.statusCode).toBe(413);
    expect(response.json()).toEqual({
      error: 'Chunk 0 content is too large: maximum is 8000 characters'
    });
    expect(reserveApiQuotaMock).not.toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(embedBatchMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects OpenAI chunks that exceed the provider per-input estimate before embedding', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [
          { role: 'user', content: '界'.repeat(3000), timestamp: '2026-05-12T16:00:00.000Z' }
        ]
      }
    });

    expect(response.statusCode).toBe(413);
    expect(response.json()).toEqual({
      error: 'Chunk 0 content is too large for embedding: estimated token maximum is 8192'
    });
    expect(reserveApiQuotaMock).not.toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(embedBatchMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('does not apply OpenAI token preflight to non-OpenAI embedders', async () => {
    configMock.EMBEDDER_PROVIDER = 'ollama';
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());

    const chunk = {
      role: 'user',
      content: '界'.repeat(3000),
      timestamp: '2026-05-12T16:00:00.000Z'
    };
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    configurePersistence({ chunks: [chunk] });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [chunk]
      }
    });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toMatchObject({ accepted: 1 });
    expect(reserveApiQuotaMock).toHaveBeenCalledWith(
      expect.objectContaining({ query: expect.any(Function) }),
      '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      'ingest_events',
      'api'
    );
    expect(embedBatchMock).toHaveBeenCalledWith([chunk.content], {
      vaultId: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      modelRole: 'embedding',
      source: 'api',
      inputType: 'document'
    });
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('does not embed or write rows when the quota precheck rejects new content', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    checkQuotaMock.mockRejectedValueOnce(new Error('ingest_events quota exceeded'));

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [
          { role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00.000Z' }
        ]
      }
    });

    expect(response.statusCode).toBe(500);
    expect(embedBatchMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls.every(([sql]) => String(sql).startsWith('SELECT'))).toBe(true);
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('does not reserve or refund quota when embedding fails', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    embedBatchMock.mockRejectedValueOnce(new Error('embedding failed'));

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [
          { role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00.000Z' }
        ]
      }
    });

    expect(response.statusCode).toBe(500);
    expect(reserveApiQuotaMock).not.toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls.every(([sql]) => String(sql).startsWith('SELECT'))).toBe(true);

    await app.close();
  });

  it('does not apply compensating refunds when persistence fails', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    configurePersistence({
      chunks: [{ timestamp: '2026-05-12T16:00:00.000Z' }],
      failRawInsert: new Error('database failed')
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [
          { role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00.000Z' }
        ]
      }
    });

    expect(response.statusCode).toBe(500);
    expect(embedBatchMock).toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(rawChunkDeleteMock).toHaveBeenCalledTimes(1);

    await app.close();
  });

  it('does not delete a blob when transaction acknowledgement is ambiguous but SQL references it', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    configurePersistence({
      chunks: [{ timestamp: '2026-05-12T16:00:00.000Z' }],
      failRawInsert: new Error('commit acknowledgement lost'),
      referencedOnFailure: true
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'ambiguous-session',
        chunks: [{
          role: 'user',
          content: 'committed content',
          timestamp: '2026-05-12T16:00:00.000Z',
          source_event: { namespace: 'openclaw-capture', id: 'message-1', ordinal: 0 }
        }]
      }
    });

    expect(response.statusCode).toBe(500);
    expect(rawChunkDeleteMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('SELECT DISTINCT blob_key'))).toBe(true);

    await app.close();
  });

  it('cleans up successful raw chunk blob writes when a later blob write fails', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    const chunks = [
      { role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00.000Z' },
      { role: 'assistant', content: 'hi', timestamp: '2026-05-12T16:00:01.000Z' }
    ];
    embedBatchMock.mockResolvedValueOnce(chunks.map(() => [1, 0]));
    const writtenKeys: string[] = [];
    rawChunkPutMock.mockImplementation(async (key: string) => {
      writtenKeys.push(key);
      if (writtenKeys.length === 2) {
        throw new Error('blob write failed');
      }
      return { blobStore: 'local', blobKey: key };
    });
    configurePersistence({ chunks });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks
      }
    });

    expect(response.statusCode).toBe(500);
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('WITH input AS'))).toBe(false);
    expect(rawChunkDeleteMock).toHaveBeenCalledTimes(2);
    expect(rawChunkDeleteMock).toHaveBeenNthCalledWith(1, writtenKeys[0]);
    expect(rawChunkDeleteMock).toHaveBeenNthCalledWith(2, writtenKeys[1]);

    await app.close();
  });

  it('surfaces rollback failures when partial raw chunk blob cleanup fails', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    const chunks = [
      { role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00.000Z' },
      { role: 'assistant', content: 'hi', timestamp: '2026-05-12T16:00:01.000Z' }
    ];
    embedBatchMock.mockResolvedValueOnce(chunks.map(() => [1, 0]));
    const attemptedKeys: string[] = [];
    rawChunkPutMock.mockImplementation(async (key: string) => {
      attemptedKeys.push(key);
      if (attemptedKeys.length === 2) {
        throw new Error('blob write failed');
      }
      return { blobStore: 'local', blobKey: key };
    });
    rawChunkDeleteMock.mockRejectedValueOnce(new Error('delete failed'));
    configurePersistence({ chunks });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks
      }
    });

    expect(response.statusCode).toBe(500);
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('WITH input AS'))).toBe(false);
    expect(rawChunkDeleteMock).toHaveBeenCalledTimes(2);
    expect(rawChunkDeleteMock).toHaveBeenNthCalledWith(1, attemptedKeys[0]);
    expect(rawChunkDeleteMock).toHaveBeenNthCalledWith(2, attemptedKeys[1]);

    await app.close();
  });

  it('keeps accepted bulk ingest successful when worker trigger fails after persistence', async () => {
    const app = Fastify({ logger: false });
    const triggerExtraction = vi.fn(() => {
      throw new Error('worker unavailable');
    });
    await registerIngestRoutes(app, triggerExtraction);

    const chunk = {
      role: 'user',
      content: 'hello',
      timestamp: '2026-05-12T16:00:00.000Z'
    };
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    configurePersistence({ chunks: [chunk] });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-session',
        chunks: [chunk]
      }
    });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toMatchObject({ accepted: 1, job_id: expect.any(String) });
    expect(triggerExtraction).toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('derives stable chunk and bulk-job identities from source events across response-loss retries', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    const chunk = {
      role: 'user',
      content: 'stable retry payload',
      timestamp: '2026-05-12T16:00:00.000Z',
      source_event: { namespace: 'persistio-replay-v2', id: 'event-stable', ordinal: 7 }
    };
    embedBatchMock.mockResolvedValue([[1, 0]]);
    const durableByKey = new Map<string, {
      id: string;
      created_at: string;
      source_event_key: string;
      source_event_payload_sha256: string;
      blob_key: string;
    }>();
    const jobIds: string[] = [];
    queryMock.mockImplementation(async (sql: string, parameters?: unknown[]) => {
      const text = String(sql);
      const params = parameters ?? [];
      if (text.includes('INSERT INTO raw_chunk_blob_write_intents')) {
        return { rowCount: (params[2] as string[]).length, rows: [] };
      }
      if (text.includes('FROM raw_chunk_blob_write_intents') && text.includes('FOR UPDATE')) {
        const keys = params[1] as string[];
        return { rowCount: keys.length, rows: keys.map((blob_key) => ({ blob_key })) };
      }
      if (text.includes('INSERT INTO jobs')) {
        const id = String(params[0]);
        jobIds.push(id);
        return { rowCount: jobIds.length === 1 ? 1 : 0, rows: jobIds.length === 1 ? [{ id }] : [] };
      }
      if (text.includes('WITH input AS')) {
        const ids = params[2] as string[];
        const blobKeys = params[5] as string[];
        const timestamps = params[8] as string[];
        const sourceKeys = params[13] as string[];
        const payloadHashes = params[15] as string[];
        const rows = sourceKeys.flatMap((sourceKey, index) => {
          if (durableByKey.has(sourceKey)) return [];
          const row = {
            id: ids[index],
            created_at: timestamps[index],
            source_event_key: sourceKey,
            source_event_payload_sha256: payloadHashes[index],
            blob_key: blobKeys[index]
          };
          durableByKey.set(sourceKey, row);
          return [{ id: row.id, created_at: row.created_at, input_index: index }];
        });
        return { rowCount: rows.length, rows };
      }
      if (text.includes('SELECT id,created_at,source_event_key')) {
        const keys = params[1] as string[];
        const rows = keys.flatMap((key) => durableByKey.get(key) ?? []);
        return { rowCount: rows.length, rows };
      }
      if (text.includes('SELECT DISTINCT blob_key')) {
        const keys = new Set(params[1] as string[]);
        const rows = [...durableByKey.values()].filter((row) => keys.has(row.blob_key));
        return { rowCount: rows.length, rows: rows.map(({ blob_key }) => ({ blob_key })) };
      }
      return { rowCount: 1, rows: [] };
    });

    const request = {
      method: 'POST' as const,
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: { session_id: 'bulk-session', chunks: [chunk] }
    };
    const first = await app.inject(request);
    const second = await app.inject(request);

    expect(first.statusCode).toBe(202);
    expect(second.statusCode).toBe(202);
    expect(second.json().job_id).toBe(first.json().job_id);
    expect(second.json().chunks[0].id).toBe(first.json().chunks[0].id);
    expect(first.json()).toMatchObject({ inserted: 1, replayed: 0 });
    expect(second.json()).toMatchObject({ inserted: 0, replayed: 1 });
    expect(new Set(jobIds).size).toBe(1);
    expect(queryMock.mock.calls.filter(([sql]) => String(sql).includes('INSERT INTO segments'))).toHaveLength(1);
    expect(queryMock.mock.calls.filter(([sql]) => String(sql).includes('INSERT INTO extraction_queue'))).toHaveLength(1);

    await app.close();
  });

  it('returns the stable replay receipt without provider calls, quota or new extraction', async () => {
    const app = Fastify();
    const triggerExtraction = vi.fn();
    await registerIngestRoutes(app, triggerExtraction);
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    const sourceEvent = { namespace: 'persistio-replay-v2', id: 'event-123', ordinal: 0 };
    const sourceKey = sourceEventKey('5a3b3e77-cbd8-48f3-98fd-095f8fcb6070', sourceEvent);
    const existingId = crypto.randomUUID();
    configurePersistence({
      chunks: [{ timestamp: '2026-05-12T16:00:00.000Z' }],
      insertedIndexes: [],
      existingRows: [{
        id: existingId,
        created_at: '2026-05-12T16:00:00.000Z',
        source_event_key: sourceKey,
        source_event_payload_sha256: ingestPayloadHash({role:'user',content:'historical payload',timestamp:'2026-05-12T16:00:00.000Z'},
          {session_id:'bulk-replay-session',trigger_type:'backfill'})
      }]
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest/bulk',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'bulk-replay-session',
        context: { trigger_type: 'backfill' },
        chunks: [{
          role: 'user',
          content: 'historical payload',
          timestamp: '2026-05-12T16:00:00.000Z',
          source_event: sourceEvent
        }]
      }
    });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toMatchObject({
      accepted: 1,
      inserted: 0,
      replayed: 1,
      chunks: [{ id: existingId, outcome: 'replayed' }]
    });
    expect(rawChunkDeleteMock).not.toHaveBeenCalled();
    expect(rawChunkPutMock).not.toHaveBeenCalled();
    expect(embedBatchMock).not.toHaveBeenCalled();
    expect(reserveApiQuotaMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('WITH input AS'))).toBe(false);
    expect(triggerExtraction).not.toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects reuse of one source-event ordinal for different content', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    const sourceEvent = { namespace: 'openclaw-capture', id: 'message-1', ordinal: 0 };
    const sourceKey = sourceEventKey('5a3b3e77-cbd8-48f3-98fd-095f8fcb6070', sourceEvent);
    configurePersistence({
      chunks: [{ timestamp: '2026-05-12T16:00:00.000Z' }],
      insertedIndexes: [],
      existingRows: [{
        id: crypto.randomUUID(),
        created_at: '2026-05-12T16:00:00.000Z',
        source_event_key: sourceKey,
        source_event_payload_sha256: 'f'.repeat(64)
      }]
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/ingest',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        session_id: 'collision-session',
        chunks: [{
          role: 'user',
          content: 'different content',
          timestamp: '2026-05-12T16:00:00.000Z',
          source_event: sourceEvent
        }]
      }
    });

    expect(response.statusCode).toBe(409);
    expect(rawChunkDeleteMock).not.toHaveBeenCalled();
    expect(rawChunkPutMock).not.toHaveBeenCalled();
    expect(embedBatchMock).not.toHaveBeenCalled();

    await app.close();
  });
});
