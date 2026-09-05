import type { FastifyReply, FastifyRequest } from 'fastify';

import { recordCustomerMetric } from './customer-metrics';

declare module 'fastify' {
  interface FastifyRequest {
    customerMetricVaultId?: string;
  }
}

const apiRequestOperations: Record<string, Partial<Record<string, string>>> = {
  '/admin/plans': {
    GET: 'admin.plans.list',
    POST: 'admin.plans.create'
  },
  '/admin/plans/:id': {
    DELETE: 'admin.plans.delete',
    GET: 'admin.plans.read',
    PATCH: 'admin.plans.update'
  },
  '/admin/vaults': {
    GET: 'admin.vaults.list',
    POST: 'admin.vaults.create'
  },
  '/admin/vaults/:id': {
    DELETE: 'admin.vaults.delete',
    GET: 'admin.vaults.read',
    PATCH: 'admin.vaults.update'
  },
  '/admin/vaults/:id/curation': {
    GET: 'admin.vaults.curation.read'
  },
  '/admin/vaults/:id/memories/graph': {
    GET: 'admin.vaults.memories.graph'
  },
  '/admin/vaults/:id/rotate-key': {
    POST: 'admin.vaults.rotate_key'
  },
  '/admin/vaults/:id/stats': {
    GET: 'admin.vaults.stats.read'
  },
  '/stats': {
    GET: 'stats.read'
  },
  '/v1/curation': {
    GET: 'curation.read'
  },
  '/v1/extract': {
    POST: 'extract'
  },
  '/v1/ingest': {
    POST: 'ingest'
  },
  '/v1/ingest/bulk': {
    POST: 'ingest.bulk'
  },
  '/v1/jobs/:id': {
    GET: 'jobs.read'
  },
  '/v1/memories': {
    GET: 'memories.list',
    POST: 'memories.create'
  },
  '/v1/memories/:id': {
    DELETE: 'memories.delete',
    GET: 'memories.read',
    PATCH: 'memories.update'
  },
  '/v1/memories/graph': {
    GET: 'memories.graph'
  },
  '/v1/recall': {
    POST: 'recall'
  }
};

export function customerMetricOperationForRoute(route: string, method: string): string | null {
  return apiRequestOperations[route]?.[method.toUpperCase()] ?? null;
}

export function setCustomerMetricVaultId(request: FastifyRequest, vaultId: string): void {
  request.customerMetricVaultId = vaultId;
}

export function recordCustomerApiRequestMetric(request: FastifyRequest, reply: FastifyReply): void {
  const route = request.routeOptions.url ?? request.url;
  const operation = customerMetricOperationForRoute(route, request.method);
  if (!operation) return;

  const workspaceId = request.vault?.account_id ?? request.auth?.account_id ?? null;
  if (!workspaceId) return;
  const vaultId = getMetricVaultId(request, route);

  try {
    recordCustomerMetric({
      api_request_count: 1,
      duration_ms: normalizeDurationMs(reply.elapsedTime),
      event_type: 'api_request',
      labels: buildApiRequestLabels(request),
      method: request.method,
      operation,
      route,
      source: 'api',
      status_code: reply.statusCode,
      ...(vaultId ? { vault_id: vaultId } : {}),
      workspace_id: workspaceId
    });
  } catch (error) {
    request.log.warn({
      error: error instanceof Error ? error.message : String(error),
      route
    }, 'failed to record customer API request metric');
  }
}

function buildApiRequestLabels(request: FastifyRequest): Record<string, string> | undefined {
  if (!request.auth?.method) return undefined;
  return {
    auth_method: request.auth.method
  };
}

function getMetricVaultId(request: FastifyRequest, route: string): string | undefined {
  if (request.vault?.id) return request.vault.id;
  if (route.startsWith('/admin/vaults/:id')) return request.customerMetricVaultId;
  if (route === '/admin/vaults' && request.method === 'POST') return request.customerMetricVaultId;
  if (request.customerMetricVaultId) return request.customerMetricVaultId;
  if (request.auth?.vault_id) return request.auth.vault_id;
  return undefined;
}

function normalizeDurationMs(durationMs: number): number {
  return Number.isFinite(durationMs) && durationMs >= 0 ? durationMs : 0;
}
