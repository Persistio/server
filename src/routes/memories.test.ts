import Fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { embedMock, enforceMemoryCreationLimitMock, queryMock, recordMemoryCountDeltaMock } = vi.hoisted(() => ({
  embedMock: vi.fn(),
  enforceMemoryCreationLimitMock: vi.fn(),
  queryMock: vi.fn(),
  recordMemoryCountDeltaMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

vi.mock('../services/embedder', () => ({
  getEmbedder: () => ({
    embed: embedMock
  })
}));

vi.mock('../services/usage', () => ({
  enforceMemoryCreationLimit: enforceMemoryCreationLimitMock,
  recordMemoryCountDelta: recordMemoryCountDeltaMock
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
      account_id: 'account-1',
      encrypted_dek: null,
      vault_encryption_enabled: false
    }]
  };
}

function graphPlanResult(enabled = true) {
  return {
    rowCount: 1,
    rows: [{
      limits: { graphEnabled: enabled }
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

function resetRouteMocks() {
  process.env.DATABASE_URL ??= 'postgres://example.com/test';
  process.env.ADMIN_API_KEY ??= 'test-admin-key';
  process.env.OPENAI_API_KEY ??= 'test-openai-key';
  embedMock.mockReset();
  embedMock.mockResolvedValue([0.1, 0.2]);
  enforceMemoryCreationLimitMock.mockReset();
  recordMemoryCountDeltaMock.mockReset();
  queryMock.mockReset();
}

describe('memory list route', () => {
  beforeEach(() => {
    resetRouteMocks();
  });

  it('returns source timestamps from the default list query', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ total: 1 }]
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
    expect(queryMock).toHaveBeenNthCalledWith(2, expect.stringContaining('memories.source_timestamp'), [vaultId, 50, 0]);
    expect(queryMock).toHaveBeenNthCalledWith(3, expect.stringContaining('COUNT(*)::int AS total'), [vaultId]);

    await app.close();
  });

  it('returns source timestamps when listing with child memories', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ total: 1 }]
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
    expect(queryMock).toHaveBeenNthCalledWith(2, expect.stringContaining('tree.source_timestamp'), [vaultId]);
    expect(queryMock).toHaveBeenNthCalledWith(3, expect.stringContaining('COUNT(*)::int AS total'), [vaultId]);

    await app.close();
  });

  it('returns subject summaries for the whole vault', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        subject: 'persistio deployment',
        subject_encrypted: null,
        count: 7,
        latest_at: '2026-05-12T16:00:00.000Z'
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/memories/subjects?q=persistio&sort=name',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      items: [
        {
          subject: 'persistio deployment',
          count: 7,
          latest_at: '2026-05-12T16:00:00.000Z'
        }
      ],
      limit: 200,
      offset: 0,
      total: 1
    });
    expect(queryMock).toHaveBeenNthCalledWith(2, expect.stringContaining('GROUP BY COALESCE(subject_hmac, subject'), [vaultId]);

    await app.close();
  });
});

describe('admin memory list route', () => {
  beforeEach(() => {
    resetRouteMocks();
  });

  it('lists memories for disabled vaults through delegated platform auth', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: vaultId,
        name: 'Example',
        purpose: null,
        settings: {},
        plan_id: 'unlimited',
        status: 'disabled',
        account_id: 'account-1',
        encrypted_dek: null,
        vault_encryption_enabled: false
      }]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow({ categories: ['preference'] })]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ total: 1 }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/admin/vaults/${vaultId}/memories?limit=20&offset=40&category=preference`,
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      items: [
        {
          id: seedMemoryId,
          subject: 'persistio',
          categories: ['preference']
        }
      ],
      limit: 20,
      offset: 40
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.not.stringContaining(`status = 'active'`),
      [vaultId]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('$2 = ANY(categories)'),
      [vaultId, 'preference', 20, 40]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining('COUNT(*)::int AS total'),
      [vaultId, 'preference']
    );

    await app.close();
  });
});

describe('admin memory graph route', () => {
  beforeEach(() => {
    resetRouteMocks();
  });

  it('returns graph data for disabled vaults through delegated platform auth', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: vaultId,
        name: 'Example',
        purpose: null,
        settings: {},
        plan_id: 'unlimited',
        status: 'disabled',
        account_id: 'account-1',
        encrypted_dek: null,
        vault_encryption_enabled: false
      }]
    });
    queryMock.mockResolvedValueOnce(graphPlanResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/admin/vaults/${vaultId}/memories/graph?limit=10&depth=4`,
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      seed_memory_id: null,
      depth: 4,
      limit: 10,
      nodes: [{ id: seedMemoryId, subject: 'persistio' }],
      edges: []
    });
    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.not.stringContaining(`status = 'active'`),
      [vaultId]
    );

    await app.close();
  });

  it('rejects delegated graph reads when the vault plan is not graph-enabled', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: vaultId,
        name: 'Example',
        purpose: null,
        settings: {},
        plan_id: 'free',
        status: 'active',
        account_id: 'account-1',
        encrypted_dek: null,
        vault_encryption_enabled: false
      }]
    });
    queryMock.mockResolvedValueOnce(graphPlanResult(false));

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/admin/vaults/${vaultId}/memories/graph?depth=not-a-number`,
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(403);
    expect(response.json()).toEqual({ error: 'Memory graph requires a graph-capable plan' });
    expect(queryMock).toHaveBeenCalledTimes(2);

    await app.close();
  });
});

describe('memory read route', () => {
  beforeEach(() => {
    resetRouteMocks();
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

describe('memory write route', () => {
  beforeEach(() => {
    resetRouteMocks();
  });

  it('records API memory count after a durable create and before embedding sync', async () => {
    queryMock.mockImplementation(async (sql: string) => {
      if (sql.includes('FROM vaults')) return authResult();
      if (sql.includes('INSERT INTO memories')) {
        return { rowCount: 1, rows: [memoryRow({ data: 'New durable memory.', subject: 'persistio' })] };
      }
      if (sql.includes('INSERT INTO memory_embeddings')) return { rowCount: 1, rows: [] };
      return { rowCount: 0, rows: [] };
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'POST',
      url: '/v1/memories',
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        data: 'New durable memory.',
        subject: 'persistio'
      }
    });

    expect(response.statusCode).toBe(201);
    expect(recordMemoryCountDeltaMock).toHaveBeenCalledWith(vaultId, 'account-1', 1, 'api');

    const memoryInsertIndex = queryMock.mock.calls.findIndex(([sql]) => String(sql).includes('INSERT INTO memories'));
    const embeddingInsertIndex = queryMock.mock.calls.findIndex(([sql]) => String(sql).includes('INSERT INTO memory_embeddings'));
    expect(memoryInsertIndex).toBeGreaterThanOrEqual(0);
    expect(embeddingInsertIndex).toBeGreaterThan(memoryInsertIndex);
    expect(recordMemoryCountDeltaMock.mock.invocationCallOrder[0]).toBeGreaterThan(queryMock.mock.invocationCallOrder[memoryInsertIndex]);
    expect(recordMemoryCountDeltaMock.mock.invocationCallOrder[0]).toBeLessThan(queryMock.mock.invocationCallOrder[embeddingInsertIndex]);

    await app.close();
  });

  it('locks the delete target before deriving archive count deltas', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: seedMemoryId,
        archived_at: '2026-06-01T12:00:00.000Z',
        previous_archived_at: null
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'DELETE',
      url: `/v1/memories/${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).not.toHaveProperty('previous_archived_at');
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(/SELECT id, archived_at[\s\S]+FOR UPDATE[\s\S]+COALESCE\(memories\.archived_at, now\(\)\)/),
      [vaultId, seedMemoryId]
    );
    expect(recordMemoryCountDeltaMock).toHaveBeenCalledWith(vaultId, 'account-1', -1, 'api');

    await app.close();
  });

  it('computes patch archive deltas from the locked update result', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow()]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [memoryRow({
        archived_at: '2026-06-01T12:00:00.000Z',
        previous_archived_at: null
      })]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: `/v1/memories/${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' },
      payload: { archived: true }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).not.toHaveProperty('previous_archived_at');
    expect(queryMock).toHaveBeenNthCalledWith(
      3,
      expect.stringMatching(/FOR UPDATE[\s\S]+archived_at = CASE[\s\S]+target\.archived_at AS previous_archived_at/),
      expect.any(Array)
    );
    const patchValues = queryMock.mock.calls[2][1] as unknown[];
    expect(patchValues[0]).toBe(vaultId);
    expect(patchValues[1]).toBe(seedMemoryId);
    expect(patchValues.at(-2)).toBe(true);
    expect(patchValues.at(-1)).toBe(true);
    expect(recordMemoryCountDeltaMock).toHaveBeenCalledWith(vaultId, 'account-1', -1, 'api');

    await app.close();
  });

  it('records patch archive deltas before embedding sync can fail', async () => {
    queryMock.mockImplementation(async (sql: string) => {
      if (sql.includes('FROM vaults')) return authResult();
      if (sql.includes('SELECT id, data, subject')) {
        return { rowCount: 1, rows: [memoryRow()] };
      }
      if (sql.includes('WITH target')) {
        return {
          rowCount: 1,
          rows: [memoryRow({
            data: 'Updated durable memory.',
            archived_at: '2026-06-01T12:00:00.000Z',
            previous_archived_at: null
          })]
        };
      }
      if (sql.includes('INSERT INTO memory_embeddings')) {
        throw new Error('embedding sync failed');
      }
      return { rowCount: 0, rows: [] };
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: `/v1/memories/${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' },
      payload: {
        data: 'Updated durable memory.',
        archived: true
      }
    });

    expect(response.statusCode).toBe(500);
    expect(recordMemoryCountDeltaMock).toHaveBeenCalledWith(vaultId, 'account-1', -1, 'api');

    const patchUpdateIndex = queryMock.mock.calls.findIndex(([sql]) => String(sql).includes('WITH target'));
    const embeddingInsertIndex = queryMock.mock.calls.findIndex(([sql]) => String(sql).includes('INSERT INTO memory_embeddings'));
    expect(patchUpdateIndex).toBeGreaterThanOrEqual(0);
    expect(embeddingInsertIndex).toBeGreaterThan(patchUpdateIndex);
    expect(recordMemoryCountDeltaMock.mock.invocationCallOrder[0]).toBeGreaterThan(queryMock.mock.invocationCallOrder[patchUpdateIndex]);
    expect(recordMemoryCountDeltaMock.mock.invocationCallOrder[0]).toBeLessThan(queryMock.mock.invocationCallOrder[embeddingInsertIndex]);

    await app.close();
  });
});

describe('memory graph route', () => {
  beforeEach(() => {
    resetRouteMocks();
  });

  it('returns a bounded seeded memory graph neighborhood', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce(graphPlanResult());
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
      3,
      expect.not.stringContaining('WITH RECURSIVE'),
      [vaultId, seedMemoryId]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      4,
      expect.stringContaining('LIMIT $6'),
      [vaultId, [seedMemoryId], [seedMemoryId], ['supports', 'contradicts'], 1, 19, 38]
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      5,
      expect.stringContaining('from_memory_id = ANY($2::uuid[])'),
      [vaultId, [seedMemoryId, neighborMemoryId], ['supports', 'contradicts']]
    );

    await app.close();
  });

  it('does not expand beyond the requested node limit', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce(graphPlanResult());
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
    expect(queryMock).toHaveBeenCalledTimes(4);
    expect(queryMock).not.toHaveBeenCalledWith(
      expect.stringContaining('candidate_edges'),
      expect.anything()
    );

    await app.close();
  });

  it('returns an overview graph when no seed memory is supplied', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce(graphPlanResult());
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
      3,
      expect.not.stringContaining('WITH RECURSIVE'),
      [vaultId, 10]
    );

    await app.close();
  });

  it('returns 404 when the seed memory is not visible in the vault', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce(graphPlanResult());
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/graph?seed_memory_id=${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Seed memory not found' });
    expect(queryMock).toHaveBeenCalledTimes(3);

    await app.close();
  });

  it('rejects direct graph reads when the vault plan is not graph-enabled', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce(graphPlanResult(false));

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: `/v1/memories/graph?seed_memory_id=${seedMemoryId}`,
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(403);
    expect(response.json()).toEqual({ error: 'Memory graph requires a graph-capable plan' });
    expect(queryMock).toHaveBeenCalledTimes(2);
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('FROM plans'),
      ['unlimited']
    );

    await app.close();
  });

  it('rejects invalid graph controls before running graph queries', async () => {
    queryMock.mockResolvedValueOnce(authResult());
    queryMock.mockResolvedValueOnce(graphPlanResult());

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/memories/graph?depth=5&edge_types=not_real',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'Invalid graph query' });
    expect(queryMock).toHaveBeenCalledTimes(2);

    await app.close();
  });
});
