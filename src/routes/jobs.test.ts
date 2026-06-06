import Fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

import { registerJobRoutes, type JobStore } from './jobs';

describe('job routes', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  async function buildApp() {
    const app = Fastify();
    const store: JobStore = {
      create: vi.fn(),
      get: vi.fn(() => undefined),
      update: vi.fn()
    };
    await registerJobRoutes(app, store, vi.fn());
    return app;
  }

  it('returns persistent bulk ingest jobs for the authenticated vault', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
        name: 'Example',
        purpose: null,
        settings: {},
        plan_id: 'unlimited',
        status: 'active',
        encrypted_dek: null,
        vault_encryption_enabled: false
      }]
    });
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{
        id: 'cead0e8f-6057-43f7-8374-9f7baf97dcef',
        vaultId: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
        status: 'running',
        createdAt: '2026-05-12T16:00:00.000Z',
        updatedAt: '2026-05-12T16:01:00.000Z',
        error: null
      }]
    });

    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/jobs/cead0e8f-6057-43f7-8374-9f7baf97dcef',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      id: 'cead0e8f-6057-43f7-8374-9f7baf97dcef',
      vaultId: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      status: 'running',
      createdAt: '2026-05-12T16:00:00.000Z',
      updatedAt: '2026-05-12T16:01:00.000Z'
    });

    await app.close();
  });
});
