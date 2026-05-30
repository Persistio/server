import Fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

import { registerMemoryRoutes } from './memories';

const vaultId = '11111111-1111-4111-8111-111111111111';
const seedMemoryId = '22222222-2222-4222-8222-222222222222';
const neighborMemoryId = '33333333-3333-4333-8333-333333333333';

function authResult() {
  return {
    rowCount: 1,
    rows: [{
      id: vaultId,
      name: 'Example',
      purpose: null,
      settings: {},
      plan_id: 'unlimited',
      status: 'active',
      encrypted_dek: null,
      vault_encryption_enabled: false
    }]
  };
}

function memoryRow(overrides: Record<string, unknown> = {}) {
  return {
    id: seedMemoryId,
    vault_id: vaultId,
    data: 'Persistio stores memories in Postgres.',
    subject: 'persistio',
    subject_encrypted: null,
    hash: 'hash',
    source_chunks: [],
    categories: [],
    confidence: 1,
    score: 5,
    salience: '0.50',
    sensitivity: 'low',
    type: 'system_fact',
    scope: 'global',
    evidence: null,
    polarity: 'neutral',
    status: 'active',
    valid_from: null,
    valid_until: null,
    source_timestamp: '2026-05-12T15:30:00.000Z',
    archived_at: null,
    created_at: '2026-05-12T16:00:00.000Z',
    updated_at: '2026-05-12T16:00:00.000Z',
    parent_id: null,
    volatility: 'low',
    edge_count: 1,
    depth: 0,
    ...overrides
  };
}

async function buildApp() {
  const app = Fastify();
  await registerMemoryRoutes(app);
  return app;
}

describe('memory list route', () => {
  beforeEach(() => {
    process.env.DATABASE_URL ??= 'postgres://example.com/test';
    process.env.ADMIN_API_KEY ??= 'test-admin-key';
    process.env.OPENAI_API_KEY ??= 'test-openai-key';
    queryMock.mockReset();
  });

  it('returns source timestamps from the default list query', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/memories',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      items: [
        {
          id: seedMemoryId,
          source_timestamp: '2026-05-12T15:30:00.000Z'
        }
      ]
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('memories.source_timestamp'),
      [vaultId, 50, 0]
    );

    await app.close();
  });

  it('returns source timestamps when listing with child memories', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/memories?include_children=true',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      items: [
        {
          id: seedMemoryId,
          source_timestamp: '2026-05-12T15:30:00.000Z'
        }
      ]
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('tree.source_timestamp'),
      [vaultId]
    );

    await app.close();
  });
});

describe('memory read route', () => {
  beforeEach(() => {
    process.env.DATABASE_URL ??= 'postgres://example.com/test';
    process.env.ADMIN_API_KEY ??= 'test-admin-key';
    process.env.OPENAI_API_KEY ??= 'test-openai-key';
    queryMock.mockReset();
  });

  it('fetches active memories directly by id without depending on the list page', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      id: seedMemoryId,
      data: 'Persistio stores memories in Postgres.',
      status: 'active'
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining(`AND (status IS NULL OR status <> 'candidate')`),
      [vaultId, seedMemoryId]
    );

    await app.close();
  });

  it('keeps candidate memories hidden from direct reads by default', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Memory not found' });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.not.stringContaining(`status = 'candidate'`),
      [vaultId, seedMemoryId]
    );

    await app.close();
  });

  it('allows fresh pending candidate reads by id when explicitly requested', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow({
        status: 'candidate',
        source_timestamp: '2026-05-30T10:00:00.000Z',
        created_at: '2026-05-30T10:00:00.000Z',
        updated_at: '2026-05-30T10:00:00.000Z'
      })]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/${seedMemoryId}?include_pending=true`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      id: seedMemoryId,
      status: 'candidate'
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining(`status = 'candidate'`),
      [vaultId, seedMemoryId, expect.any(String)]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('COALESCE(source_timestamp, created_at) >= $3::timestamptz'),
      expect.anything()
    );

    await app.close();
  });
});

describe('memory graph route', () => {
  beforeEach(() => {
    process.env.DATABASE_URL ??= 'postgres://example.com/test';
    process.env.ADMIN_API_KEY ??= 'test-admin-key';
    process.env.OPENAI_API_KEY ??= 'test-openai-key';
    queryMock.mockReset();
  });

  it('returns a bounded seeded memory graph neighborhood', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow({
        id: neighborMemoryId,
        data: 'Persistio links related durable memories.',
        subject: 'memory graph',
        depth: 1
      })]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: '44444444-4444-4444-8444-444444444444',
        from_memory_id: seedMemoryId,
        to_memory_id: neighborMemoryId,
        type: 'supports',
        confidence: 0.9,
        reason: 'Related implementation details',
        created_at: '2026-05-12T16:00:00.000Z',
        updated_at: '2026-05-12T16:00:00.000Z'
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/graph?seed_memory_id=${seedMemoryId}&depth=1&limit=20&direction=both&edge_types=supports,contradicts`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      seed_memory_id: seedMemoryId,
      depth: 1,
      limit: 20,
      direction: 'both',
      edge_types: ['supports', 'contradicts'],
      nodes: [
        { id: seedMemoryId, data: 'Persistio stores memories in Postgres.', source_timestamp: '2026-05-12T15:30:00.000Z', depth: 0 },
        { id: neighborMemoryId, data: 'Persistio links related durable memories.', depth: 1 }
      ],
      edges: [
        {
          from_memory_id: seedMemoryId,
          to_memory_id: neighborMemoryId,
          type: 'supports'
        }
      ]
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.not.stringContaining('WITH RECURSIVE'),
      [vaultId, seedMemoryId]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining('LIMIT $6'),
      [vaultId, [seedMemoryId], [seedMemoryId], ['supports', 'contradicts'], 1, 19, 38]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      4,
      expect.stringContaining('from_memory_id = ANY($2::uuid[])'),
      [vaultId, [seedMemoryId, neighborMemoryId], ['supports', 'contradicts']]
    );

    await app.close();
  });

  it('does not expand beyond the requested node limit', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/graph?seed_memory_id=${seedMemoryId}&depth=3&limit=1`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      seed_memory_id: seedMemoryId,
      depth: 3,
      limit: 1,
      nodes: [{ id: seedMemoryId, depth: 0 }],
      edges: []
    });
    expect(queryMock).toHaveBeenCalledTimes(3);
    expect(queryMock).not.toHaveBeenCalledWith(
      expect.stringContaining('candidate_edges'),
      expect.anything()
    );

    await app.close();
  });

  it('returns an overview graph when no seed memory is supplied', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/memories/graph?limit=10',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      seed_memory_id: null,
      depth: 1,
      limit: 10,
      nodes: [{ id: seedMemoryId, depth: 0 }],
      edges: []
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.not.stringContaining('WITH RECURSIVE'),
      [vaultId, 10]
    );

    await app.close();
  });

  it('returns 404 when the seed memory is not visible in the vault', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/graph?seed_memory_id=${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Seed memory not found' });
    expect(queryMock).toHaveBeenCalledTimes(2);

    await app.close();
  });

  it('rejects invalid graph controls before running graph queries', async () => {
    queryMock.mockResolvedValueOnce(authResult());

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/memories/graph?depth=4&edge_types=not_real',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'Invalid graph query' });
    expect(queryMock).toHaveBeenCalledTimes(1);

    await app.close();
  });
});
