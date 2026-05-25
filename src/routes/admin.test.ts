import Fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

import { registerAdminRoutes } from './admin';

describe('admin vault updates', () => {
  beforeEach(() => {
    process.env.DATABASE_URL ??= 'postgres://example.com/test';
    process.env.ADMIN_API_KEY ??= 'test-admin-key';
    process.env.OPENAI_API_KEY ??= 'test-openai-key';
    queryMock.mockReset();
  });

  async function buildApp() {
    const app = Fastify();
    await registerAdminRoutes(app);
    return app;
  }

  it('updates a vault plan through PATCH /admin/vaults/:id without mutating plan limits', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 'custom' }] });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        name: 'Example',
        purpose: null,
        created_at: '2026-05-15T00:00:00.000Z',
        settings: {},
        plan_id: 'custom',
        status: 'active',
        account_id: null,
        vault_encryption_enabled: false
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { plan: 'custom' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ plan_id: 'custom' });
    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('FROM plans'),
      ['custom']
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('status = COALESCE($6, status)'),
      [
        'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        null,
        false,
        null,
        'custom',
        null
      ]
    );

    await app.close();
  });

  it('rejects plan limits on vault updates', async () => {
    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { plan: 'custom', plan_limits: { memories_max: 10000 } }
    });

    expect(response.statusCode).toBe(400);
    expect(queryMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects vault plan updates when the plan does not exist', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { plan: 'missing' }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Plan not found' });
    expect(queryMock).toHaveBeenCalledTimes(1);

    await app.close();
  });

  it('creates vaults on the default unlimited plan', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 'unlimited' }] });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        name: 'Example',
        purpose: null,
        plan_id: 'unlimited',
        status: 'active',
        created_at: '2026-05-15T00:00:00.000Z'
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'POST',
      url: '/admin/vaults',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { name: 'Example' }
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toMatchObject({ plan: 'unlimited' });
    expect(queryMock).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('FROM plans'),
      ['unlimited']
    );
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('INSERT INTO vaults'),
      expect.arrayContaining(['unlimited'])
    );

    await app.close();
  });

  it('rejects plan limits on vault creation', async () => {
    const app = await buildApp();
    const response = await app.inject({
      method: 'POST',
      url: '/admin/vaults',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { name: 'Example', plan_limits: { memories_max: 10000 } }
    });

    expect(response.statusCode).toBe(400);
    expect(queryMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects vault creation when the plan does not exist', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'POST',
      url: '/admin/vaults',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { name: 'Example', plan: 'missing' }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Plan not found' });
    expect(queryMock).toHaveBeenCalledTimes(1);

    await app.close();
  });

  it('creates or updates plans through POST /admin/plans', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ id: 'unlimited', limits: { memories_max: 1000000 } }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'POST',
      url: '/admin/plans',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { id: 'unlimited', limits: { memories_max: 1000000 } }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ id: 'unlimited', limits: { memories_max: 1000000 } });
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('ON CONFLICT (id) DO UPDATE'),
      ['unlimited', JSON.stringify({ memories_max: 1000000 })]
    );

    await app.close();
  });

  it('lists and reads platform plans', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ id: 'unlimited', limits: { memories_max: 1000000 } }]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ id: 'unlimited', limits: { memories_max: 1000000 } }]
    });

    const app = await buildApp();
    const listResponse = await app.inject({
      method: 'GET',
      url: '/admin/plans',
      headers: { 'x-admin-key': 'test-admin-key' }
    });
    const getResponse = await app.inject({
      method: 'GET',
      url: '/admin/plans/unlimited',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(listResponse.statusCode).toBe(200);
    expect(listResponse.json()).toEqual({ items: [{ id: 'unlimited', limits: { memories_max: 1000000 } }] });
    expect(getResponse.statusCode).toBe(200);
    expect(getResponse.json()).toEqual({ id: 'unlimited', limits: { memories_max: 1000000 } });

    await app.close();
  });

  it('updates plan limits through PATCH /admin/plans/:id', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ id: 'unlimited', limits: { memories_max: 2000000 } }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/plans/unlimited',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { limits: { memories_max: 2000000 } }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ id: 'unlimited', limits: { memories_max: 2000000 } });
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('UPDATE plans'),
      ['unlimited', JSON.stringify({ memories_max: 2000000 })]
    );

    await app.close();
  });

  it('returns not found when updating a missing plan', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/plans/missing',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { limits: { memories_max: 2000000 } }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Plan not found' });

    await app.close();
  });

  it('does not delete plans that are referenced by vaults', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ count: '2' }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'DELETE',
      url: '/admin/plans/unlimited',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(409);
    expect(response.json()).toEqual({ error: 'Plan is in use' });
    expect(queryMock).toHaveBeenCalledTimes(1);

    await app.close();
  });

  it('deletes unused plans', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ count: '0' }]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ id: 'custom' }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'DELETE',
      url: '/admin/plans/custom',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ id: 'custom', deleted: true });

    await app.close();
  });

  it('returns not found when deleting a missing plan', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ count: '0' }]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 0,
      rows: []
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'DELETE',
      url: '/admin/plans/missing',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'Plan not found' });

    await app.close();
  });

  it('returns vault detail without key material through GET /admin/vaults/:id', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        name: 'Example',
        purpose: 'Support assistant memory',
        created_at: '2026-05-15T00:00:00.000Z',
        settings: {},
        plan_id: 'unlimited',
        status: 'active',
        account_id: 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef',
        vault_encryption_enabled: false
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/admin/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      name: 'Example',
      plan_id: 'unlimited'
    });
    expect(response.json()).not.toHaveProperty('api_key');
    expect(response.json()).not.toHaveProperty('api_key_hash');

    await app.close();
  });

  it('updates vault status through PATCH /admin/vaults/:id', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        name: 'Example',
        purpose: null,
        created_at: '2026-05-15T00:00:00.000Z',
        settings: {},
        plan_id: 'free',
        status: 'disabled',
        account_id: null,
        vault_encryption_enabled: false
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { status: 'disabled' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ status: 'disabled' });
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('status = COALESCE($6, status)'),
      [
        'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        null,
        false,
        null,
        null,
        'disabled'
      ]
    );

    await app.close();
  });

});
