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

  it('updates a vault plan through PATCH /admin/vaults/:id', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        name: 'Example',
        purpose: null,
        created_at: '2026-05-15T00:00:00.000Z',
        settings: {},
        plan_id: 'pro',
        account_id: null,
        vault_encryption_enabled: false
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'PATCH',
      url: '/admin/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      headers: { 'x-admin-key': 'test-admin-key' },
      payload: { plan: 'pro' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ plan_id: 'pro' });
    expect(queryMock).toHaveBeenCalledWith(
      expect.stringContaining('plan_id = COALESCE($5, plan_id)'),
      [
        'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
        null,
        false,
        null,
        'pro'
      ]
    );

    await app.close();
  });

});
