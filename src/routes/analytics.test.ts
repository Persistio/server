import Fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const workspaceId = '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee';
const otherWorkspaceId = 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef';
const vaultId = '57a89ef6-5fee-4106-956d-1ac3cfa85dd6';

const { authContext } = vi.hoisted(() => ({
  authContext: {
    account_id: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee',
    actor: { id: 'user-1', type: 'user' },
    client_id: 'test-client',
    method: 'oauth',
    scopes: ['platform:analytics:read'],
    subject: 'client-subject',
    vault_id: null
  }
}));

vi.mock('../middleware/auth', () => ({
  requireAdminScope: () => async (request: { auth: typeof authContext }) => {
    request.auth = authContext;
  }
}));

import type { AppConfig } from '../config';
import { registerAnalyticsRoutes } from './analytics';

function appConfig(overrides: Partial<AppConfig> = {}): AppConfig {
  return {
    ANALYTICS_DAILY_MAX_RANGE_DAYS: 90,
    ANALYTICS_HOURLY_MAX_RANGE_DAYS: 14,
    ...overrides
  } as AppConfig;
}

function analyticsService() {
  return {
    getApiRequests: vi.fn().mockResolvedValue({ items: [], query: { job_bytes_billed: 1, job_bytes_processed: 1 } }),
    getModelUsage: vi.fn().mockResolvedValue({ items: [], query: { job_bytes_billed: 1, job_bytes_processed: 1 } }),
    getTopVaults: vi.fn().mockResolvedValue({ items: [], query: { job_bytes_billed: 1, job_bytes_processed: 1 } }),
    getVaultApiRequests: vi.fn().mockResolvedValue({ items: [], query: { job_bytes_billed: 1, job_bytes_processed: 1 } }),
    getVaultMetrics: vi.fn().mockResolvedValue({ items: [], query: { job_bytes_billed: 1, job_bytes_processed: 1 } }),
    getWorkspaceSummary: vi.fn().mockResolvedValue({
      items: [{ api_request_count: 10, bucket_ts: '2026-06-11T00:00:00.000Z' }],
      query: { job_bytes_billed: 1, job_bytes_processed: 1 }
    })
  };
}

describe('analytics routes', () => {
  beforeEach(() => {
    authContext.account_id = workspaceId;
    authContext.method = 'oauth';
    authContext.vault_id = null;
  });

  async function buildApp(service = analyticsService()) {
    const app = Fastify();
    await registerAnalyticsRoutes(app, appConfig(), { service: service as never });
    return { app, service };
  }

  it('returns workspace summary for the authorized workspace', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/metrics/summary?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=hour`
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      grain: 'hour',
      items: [{ api_request_count: 10 }],
      workspace_id: workspaceId
    });
    expect(service.getWorkspaceSummary).toHaveBeenCalledWith({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      workspaceId
    });

    await app.close();
  });

  it('does not register analytics routes outside the /analytics prefix', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/v1/workspaces/${workspaceId}/metrics/summary?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=hour`
    });

    expect(response.statusCode).toBe(404);
    expect(service.getWorkspaceSummary).not.toHaveBeenCalled();

    await app.close();
  });

  it('uses the default UI snapshot params for top vaults when query params are omitted', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/top-vaults?from=2026-05-12T00:00:00.000Z&to=2026-06-11T00:00:00.000Z`
    });

    expect(response.statusCode).toBe(200);
    expect(service.getTopVaults).toHaveBeenCalledWith({
      from: new Date('2026-05-12T00:00:00.000Z'),
      limit: 5,
      metric: 'api_requests',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId
    });
    expect(response.json()).toMatchObject({
      grain: 'day',
      limit: 5,
      metric: 'api_requests',
      workspace_id: workspaceId
    });

    await app.close();
  });

  it('rejects workspace analytics for a different delegated workspace', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${otherWorkspaceId}/metrics/summary?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z`
    });

    expect(response.statusCode).toBe(403);
    expect(service.getWorkspaceSummary).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects analytics access without an OAuth delegated workspace context', async () => {
    authContext.method = 'api_key';
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/metrics/summary?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z`
    });

    expect(response.statusCode).toBe(403);
    expect(service.getWorkspaceSummary).not.toHaveBeenCalled();

    await app.close();
  });

  it.each([
    {
      serviceMethod: 'getWorkspaceSummary',
      url: `/analytics/v1/workspaces/${workspaceId}/metrics/summary?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z`
    },
    {
      serviceMethod: 'getApiRequests',
      url: `/analytics/v1/workspaces/${workspaceId}/api-requests?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z`
    },
    {
      serviceMethod: 'getModelUsage',
      url: `/analytics/v1/workspaces/${workspaceId}/model-usage?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z`
    },
    {
      serviceMethod: 'getTopVaults',
      url: `/analytics/v1/workspaces/${workspaceId}/top-vaults?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z`
    }
  ])('rejects workspace rollup access for vault-scoped tokens on $serviceMethod', async ({ serviceMethod, url }) => {
    authContext.vault_id = vaultId;
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url
    });

    expect(response.statusCode).toBe(403);
    expect(service[serviceMethod as keyof typeof service]).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects hourly ranges beyond the configured maximum', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/api-requests?from=2026-06-01T00:00:00.000Z&to=2026-06-30T00:00:00.000Z&grain=hour`
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'hour queries are limited to 14 days' });
    expect(service.getApiRequests).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects hourly ranges that are not aligned to UTC hour boundaries', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/api-requests?from=2026-06-11T09:30:00.000Z&to=2026-06-11T10:30:00.000Z&grain=hour`
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'hour queries must use UTC hour boundaries' });
    expect(service.getApiRequests).not.toHaveBeenCalled();

    await app.close();
  });

  it('returns a client error for malformed workspace ids', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: '/analytics/v1/workspaces/not-a-uuid/metrics/summary?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day'
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'Invalid workspace id' });
    expect(service.getWorkspaceSummary).not.toHaveBeenCalled();

    await app.close();
  });

  it('returns a client error for malformed vault ids', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/not-a-uuid/metrics?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'Invalid workspace or vault id' });
    expect(service.getVaultMetrics).not.toHaveBeenCalled();

    await app.close();
  });

  it('rejects daily ranges that are not aligned to UTC day boundaries', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/metrics/summary?from=2026-06-11T12:00:00.000Z&to=2026-06-12T12:00:00.000Z&grain=day`
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'day queries must use UTC day boundaries' });
    expect(service.getWorkspaceSummary).not.toHaveBeenCalled();

    await app.close();
  });

  it('accepts offset datetimes when they resolve to UTC day boundaries', async () => {
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/metrics/summary?from=2026-06-11T01:00:00.000%2B01:00&to=2026-06-12T01:00:00.000%2B01:00&grain=day`
    });

    expect(response.statusCode).toBe(200);
    expect(service.getWorkspaceSummary).toHaveBeenCalledWith(expect.objectContaining({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-12T00:00:00.000Z')
    }));

    await app.close();
  });

  it('allows vault metrics when a token vault constraint matches the requested vault', async () => {
    authContext.vault_id = vaultId;
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/${vaultId}/metrics?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    });

    expect(response.statusCode).toBe(200);
    expect(service.getVaultMetrics).toHaveBeenCalledWith(expect.objectContaining({
      vaultId,
      workspaceId
    }));

    await app.close();
  });

  it.each([
    {
      serviceMethod: 'getVaultApiRequests',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/${vaultId}/api-requests?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    },
    {
      serviceMethod: 'getModelUsage',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/${vaultId}/model-usage?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    }
  ])('allows vault analytics on $serviceMethod when a token vault constraint matches', async ({ serviceMethod, url }) => {
    authContext.vault_id = vaultId;
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url
    });

    expect(response.statusCode).toBe(200);
    expect(service[serviceMethod as keyof typeof service]).toHaveBeenCalledWith(expect.objectContaining({
      vaultId,
      workspaceId
    }));

    await app.close();
  });

  it('rejects vault metrics when a token vault constraint does not match', async () => {
    authContext.vault_id = vaultId;
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3/metrics?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    });

    expect(response.statusCode).toBe(403);
    expect(service.getVaultMetrics).not.toHaveBeenCalled();

    await app.close();
  });

  it.each([
    {
      serviceMethod: 'getVaultApiRequests',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3/api-requests?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    },
    {
      serviceMethod: 'getModelUsage',
      url: `/analytics/v1/workspaces/${workspaceId}/vaults/dff718f2-9d97-43b2-a3cc-a14099ed42c3/model-usage?from=2026-06-11T00:00:00.000Z&to=2026-06-12T00:00:00.000Z&grain=day`
    }
  ])('rejects vault analytics on $serviceMethod when a token vault constraint does not match', async ({ serviceMethod, url }) => {
    authContext.vault_id = vaultId;
    const { app, service } = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url
    });

    expect(response.statusCode).toBe(403);
    expect(service[serviceMethod as keyof typeof service]).not.toHaveBeenCalled();

    await app.close();
  });
});
