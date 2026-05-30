import crypto from 'node:crypto';
import Fastify, { type FastifyReply, type FastifyRequest } from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { configMock, embedBatchMock, queryMock, refundApiQuotaReservationMock, reserveApiQuotaMock } = vi.hoisted(() => ({
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
  refundApiQuotaReservationMock: vi.fn(),
  reserveApiQuotaMock: vi.fn()
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
  withTransaction: async (callback: (client: { query: typeof queryMock }) => Promise<unknown>) => callback({ query: queryMock })
}));

vi.mock('../middleware/auth', () => ({
  requireVaultAuth: async (request: FastifyRequest, _reply: FastifyReply) => {
    request.vault = {
      id: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      name: 'Premium Eval',
      purpose: null,
      settings: {},
      plan_id: 'pro',
      status: 'active',
      encrypted_dek: null,
      vault_encryption_enabled: false
    };
  }
}));

vi.mock('../services/crypto', () => ({
  encryptForVault: async (_vault: unknown, value: string) => value
}));

vi.mock('../services/embedder', () => ({
  OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT: 8192,
  estimateEmbeddingTokens: (text: string) => Math.max(1, Buffer.byteLength(text, 'utf8')),
  getEmbedder: () => ({
    embedBatch: embedBatchMock
  })
}));

vi.mock('../services/usage', () => ({
  applyRateLimitHeaders: vi.fn(),
  refundApiQuotaReservation: refundApiQuotaReservationMock,
  reserveApiQuota: reserveApiQuotaMock,
  consumeNormalIngestRateLimit: vi.fn(() => ({
    limit: 60,
    remaining: 59,
    resetAtEpochSeconds: 1,
    retryAfterSeconds: null
  })),
  isPremiumPlan: (planId: string) => planId === 'pro' || planId === 'unlimited'
}));

vi.mock('../metrics', () => ({
  ingestChunksCounter: {
    add: vi.fn()
  }
}));

import { registerIngestRoutes } from './ingest';

describe('bulk ingest route body limit', () => {
  beforeEach(() => {
    configMock.EMBEDDER_PROVIDER = 'openai';
    embedBatchMock.mockReset();
    queryMock.mockReset();
    refundApiQuotaReservationMock.mockReset();
    reserveApiQuotaMock.mockReset();
    reserveApiQuotaMock.mockResolvedValue(quotaReservation);
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
    queryMock.mockResolvedValue({ rowCount: 1, rows: [] });
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({
        rowCount: chunks.length,
        rows: chunks.map((chunk, index) => ({
          id: crypto.randomUUID(),
          created_at: chunk.timestamp,
          role: chunk.role,
          content: chunk.content,
          embedding: [1, 0],
          index
        }))
      });

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
      '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      'ingest_events'
    );
    expect(reserveApiQuotaMock.mock.invocationCallOrder[0]).toBeLessThan(embedBatchMock.mock.invocationCallOrder[0]);
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();
    expect(queryMock.mock.calls[1][0]).toContain('WITH input AS');
    expect(queryMock.mock.calls[1][0]).toContain('ORDER BY input.ordinal');
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
    queryMock.mockResolvedValue({ rowCount: 1, rows: [] });
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: crypto.randomUUID(), created_at: chunk.timestamp }]
      });

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
    expect(reserveApiQuotaMock).toHaveBeenCalledWith('5a3b3e77-cbd8-48f3-98fd-095f8fcb6070', 'ingest_events');
    expect(embedBatchMock).toHaveBeenCalledWith([chunk.content]);
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('does not embed or write rows when atomic quota reservation fails', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    reserveApiQuotaMock.mockRejectedValueOnce(new Error('ingest_events quota exceeded'));

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
    expect(queryMock).not.toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('refunds reserved quota when embedding fails after reservation', async () => {
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
    expect(reserveApiQuotaMock).toHaveBeenCalled();
    expect(refundApiQuotaReservationMock).toHaveBeenCalledWith(quotaReservation);
    expect(queryMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('refunds reserved quota when persistence fails after embedding', async () => {
    const app = Fastify();
    await registerIngestRoutes(app, vi.fn());
    embedBatchMock.mockResolvedValueOnce([[1, 0]]);
    queryMock.mockRejectedValueOnce(new Error('database failed'));

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
    expect(refundApiQuotaReservationMock).toHaveBeenCalledWith(quotaReservation);

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
    queryMock.mockResolvedValue({ rowCount: 1, rows: [] });
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: crypto.randomUUID(), created_at: chunk.timestamp }]
      });

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
});
