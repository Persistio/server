import { randomUUID } from 'node:crypto';

import type { FastifyReply, FastifyRequest } from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { PlatformAuthContext, VaultContext } from '../middleware/auth';

const { recordCustomerMetricMock } = vi.hoisted(() => ({
  recordCustomerMetricMock: vi.fn()
}));

vi.mock('./customer-metrics', () => ({
  recordCustomerMetric: recordCustomerMetricMock
}));

import {
  customerMetricOperationForRoute,
  recordCustomerApiRequestMetric,
  setCustomerMetricVaultId
} from './customer-api-request-metrics';

const workspaceId = '28b6d958-85de-443d-bd6e-44edc1e9ed38';
const vaultId = '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070';

function vaultContext(overrides: Partial<VaultContext> = {}): VaultContext {
  return {
    account_id: workspaceId,
    encrypted_dek: null,
    id: vaultId,
    name: 'Metrics Vault',
    plan_id: 'unlimited',
    purpose: null,
    settings: {},
    status: 'active',
    vault_encryption_enabled: false,
    ...overrides
  };
}

function authContext(overrides: Partial<PlatformAuthContext> = {}): PlatformAuthContext {
  return {
    account_id: workspaceId,
    actor: null,
    client_id: null,
    method: 'api_key',
    scopes: ['vault:access'],
    subject: `vault:${vaultId}`,
    vault_id: vaultId,
    ...overrides
  };
}

function requestForMetric(input: {
  auth?: PlatformAuthContext;
  method: string;
  params?: Record<string, string>;
  route: string;
  url?: string;
  vault?: VaultContext;
}): FastifyRequest {
  return {
    auth: input.auth,
    log: {
      warn: vi.fn()
    },
    method: input.method,
    params: input.params,
    routeOptions: {
      url: input.route
    },
    url: input.url ?? input.route,
    vault: input.vault
  } as unknown as FastifyRequest;
}

function replyForMetric(input: { elapsedTime?: number; statusCode: number }): FastifyReply {
  return {
    elapsedTime: input.elapsedTime ?? 12.5,
    statusCode: input.statusCode
  } as FastifyReply;
}

describe('customerMetricOperationForRoute', () => {
  it('returns stable operation names for known customer-visible routes', () => {
    expect(customerMetricOperationForRoute('/v1/ingest', 'POST')).toBe('ingest');
    expect(customerMetricOperationForRoute('/v1/ingest/bulk', 'POST')).toBe('ingest.bulk');
    expect(customerMetricOperationForRoute('/v1/memories/:id', 'PATCH')).toBe('memories.update');
    expect(customerMetricOperationForRoute('/admin/vaults/:id/memories/graph', 'GET')).toBe('admin.vaults.memories.graph');
    expect(customerMetricOperationForRoute('/admin/vaults/:id/stats', 'GET')).toBe('admin.vaults.stats.read');
  });

  it('does not emit operations for routes outside the customer metric contract', () => {
    expect(customerMetricOperationForRoute('/health', 'GET')).toBeNull();
    expect(customerMetricOperationForRoute('/v1/memories/:id', 'POST')).toBeNull();
  });
});

describe('recordCustomerApiRequestMetric', () => {
  beforeEach(() => {
    recordCustomerMetricMock.mockReset();
  });

  it('emits a vault-scoped API request metric for successful customer requests', async () => {
    recordCustomerApiRequestMetric(
      requestForMetric({
        auth: authContext(),
        method: 'GET',
        route: '/v1/memories/:id',
        url: `/v1/memories/${randomUUID()}`,
        vault: vaultContext()
      }),
      replyForMetric({ statusCode: 200 })
    );

    expect(recordCustomerMetricMock).toHaveBeenCalledWith(expect.objectContaining({
      api_request_count: 1,
      event_type: 'api_request',
      labels: { auth_method: 'api_key' },
      method: 'GET',
      operation: 'memories.read',
      route: '/v1/memories/:id',
      source: 'api',
      status_code: 200,
      vault_id: vaultId,
      workspace_id: workspaceId
    }));
    expect(recordCustomerMetricMock.mock.calls[0][0].duration_ms).toEqual(expect.any(Number));
  });

  it('emits rate-limited API request metrics after vault attribution is known', async () => {
    recordCustomerApiRequestMetric(
      requestForMetric({
        auth: authContext(),
        method: 'POST',
        route: '/v1/recall',
        vault: vaultContext()
      }),
      replyForMetric({ statusCode: 429 })
    );

    expect(recordCustomerMetricMock).toHaveBeenCalledWith(expect.objectContaining({
      operation: 'recall',
      route: '/v1/recall',
      status_code: 429,
      vault_id: vaultId,
      workspace_id: workspaceId
    }));
  });

  it('emits workspace and target vault metrics for delegated admin vault routes', async () => {
    const targetVaultId = randomUUID();
    const request = requestForMetric({
      auth: authContext({
        method: 'oauth',
        scopes: ['platform:vaults:stats:read'],
        subject: 'auth0|admin',
        vault_id: null
      }),
      method: 'GET',
      params: { id: targetVaultId },
      route: '/admin/vaults/:id/stats',
      url: `/admin/vaults/${targetVaultId}/stats`
    });
    setCustomerMetricVaultId(request, targetVaultId);

    recordCustomerApiRequestMetric(request, replyForMetric({ statusCode: 200 }));

    expect(recordCustomerMetricMock).toHaveBeenCalledWith(expect.objectContaining({
      labels: { auth_method: 'oauth' },
      operation: 'admin.vaults.stats.read',
      route: '/admin/vaults/:id/stats',
      status_code: 200,
      vault_id: targetVaultId,
      workspace_id: workspaceId
    }));
  });

  it('emits workspace and target vault metrics for delegated memory graph reads', async () => {
    const targetVaultId = randomUUID();
    const request = requestForMetric({
      auth: authContext({
        method: 'oauth',
        scopes: ['platform:vaults:read'],
        subject: 'auth0|admin',
        vault_id: null
      }),
      method: 'GET',
      params: { id: targetVaultId },
      route: '/admin/vaults/:id/memories/graph',
      url: `/admin/vaults/${targetVaultId}/memories/graph`
    });
    setCustomerMetricVaultId(request, targetVaultId);

    recordCustomerApiRequestMetric(request, replyForMetric({ statusCode: 200 }));

    expect(recordCustomerMetricMock).toHaveBeenCalledWith(expect.objectContaining({
      labels: { auth_method: 'oauth' },
      operation: 'admin.vaults.memories.graph',
      route: '/admin/vaults/:id/memories/graph',
      status_code: 200,
      vault_id: targetVaultId,
      workspace_id: workspaceId
    }));
  });

  it('does not trust admin vault route params before the handler confirms access', async () => {
    const targetVaultId = randomUUID();
    recordCustomerApiRequestMetric(
      requestForMetric({
        auth: authContext({
          method: 'oauth',
          scopes: ['platform:vaults:stats:read'],
          subject: 'auth0|admin',
          vault_id: randomUUID()
        }),
        method: 'GET',
        params: { id: targetVaultId },
        route: '/admin/vaults/:id/stats',
        url: `/admin/vaults/${targetVaultId}/stats`
      }),
      replyForMetric({ statusCode: 404 })
    );

    expect(recordCustomerMetricMock).toHaveBeenCalledWith(expect.objectContaining({
      operation: 'admin.vaults.stats.read',
      route: '/admin/vaults/:id/stats',
      status_code: 404,
      workspace_id: workspaceId
    }));
    expect(recordCustomerMetricMock.mock.calls[0][0]).not.toHaveProperty('vault_id');
  });

  it('skips routes that cannot be attributed to a workspace', async () => {
    recordCustomerApiRequestMetric(
      requestForMetric({
        auth: authContext({ account_id: null }),
        method: 'GET',
        route: '/v1/memories/:id',
        url: `/v1/memories/${randomUUID()}`,
        vault: vaultContext({ account_id: null })
      }),
      replyForMetric({ statusCode: 200 })
    );

    expect(recordCustomerMetricMock).not.toHaveBeenCalled();
  });

  it('skips non-customer operational routes', async () => {
    recordCustomerApiRequestMetric(
      requestForMetric({
        auth: authContext(),
        method: 'GET',
        route: '/health',
        vault: vaultContext()
      }),
      replyForMetric({ statusCode: 200 })
    );

    expect(recordCustomerMetricMock).not.toHaveBeenCalled();
  });
});
