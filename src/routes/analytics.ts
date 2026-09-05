import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';

import type { AppConfig } from '../config';
import { requireAdminScope } from '../middleware/auth';
import {
  DEFAULT_TOP_VAULT_LIMIT,
  DEFAULT_TOP_VAULT_METRIC
} from '../services/analytics-defaults';
import {
  CustomerAnalyticsService,
  type AnalyticsGrain,
} from '../services/customer-analytics';

const uuidSchema = z.string().uuid().transform((value) => value.toLowerCase());
const grainSchema = z.enum(['hour', 'day']).default('day');
const topVaultMetricSchema = z.enum([
  'api_requests',
  'errors',
  'rate_limited',
  'searches',
  'ingest_events',
  'memory_adds',
  'memory_count'
]).default(DEFAULT_TOP_VAULT_METRIC);
const analyticsRoutePrefix = '/analytics';

const baseQuerySchema = z.object({
  from: z.string().datetime({ offset: true }),
  grain: grainSchema,
  to: z.string().datetime({ offset: true })
}).strict();

const topVaultQuerySchema = z.object({
  from: z.string().datetime({ offset: true }),
  limit: z.coerce.number().int().min(1).max(50).default(DEFAULT_TOP_VAULT_LIMIT),
  metric: topVaultMetricSchema,
  to: z.string().datetime({ offset: true })
}).strict();
const workspaceParamsSchema = z.object({ workspaceId: uuidSchema });
const vaultParamsSchema = z.object({
  vaultId: uuidSchema,
  workspaceId: uuidSchema
});

type ParsedParams<T> = { data: T; ok: true } | { ok: false };

interface AnalyticsRouteOptions {
  service?: CustomerAnalyticsService;
}

export async function registerAnalyticsRoutes(
  app: FastifyInstance,
  config: AppConfig,
  options: AnalyticsRouteOptions = {}
) {
  const service = options.service ?? new CustomerAnalyticsService(config, undefined, undefined, app.log);
  const analyticsAuth = requireAdminScope('platform:analytics:read');

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/metrics/summary`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseWorkspaceParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace id');
    if (!hasWorkspaceRollupAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    const parsedQuery = baseQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, parsedQuery.data.grain, config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: parsedQuery.data.grain, workspace_id: params.data.workspaceId });

    const result = await service.getWorkspaceSummary({
      from: range.from,
      grain: parsedQuery.data.grain,
      to: range.to,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: parsedQuery.data.grain,
      items: result.items,
      query: result.query,
      to: range.to.toISOString(),
      workspace_id: params.data.workspaceId
    };
  });

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/vaults/:vaultId/metrics`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseVaultParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace or vault id');
    if (!hasWorkspaceAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    if (!hasVaultAccess(request, params.data.vaultId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });
    const parsedQuery = baseQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, parsedQuery.data.grain, config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: parsedQuery.data.grain, vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });

    const result = await service.getVaultMetrics({
      from: range.from,
      grain: parsedQuery.data.grain,
      to: range.to,
      vaultId: params.data.vaultId,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: parsedQuery.data.grain,
      items: result.items,
      query: result.query,
      to: range.to.toISOString(),
      vault_id: params.data.vaultId,
      workspace_id: params.data.workspaceId
    };
  });

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/model-usage`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseWorkspaceParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace id');
    if (!hasWorkspaceRollupAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    const parsedQuery = baseQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, parsedQuery.data.grain, config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: parsedQuery.data.grain, workspace_id: params.data.workspaceId });

    const result = await service.getModelUsage({
      from: range.from,
      grain: parsedQuery.data.grain,
      to: range.to,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: parsedQuery.data.grain,
      items: result.items,
      query: result.query,
      to: range.to.toISOString(),
      workspace_id: params.data.workspaceId
    };
  });

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/vaults/:vaultId/api-requests`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseVaultParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace or vault id');
    if (!hasWorkspaceAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    if (!hasVaultAccess(request, params.data.vaultId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });
    const parsedQuery = baseQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, parsedQuery.data.grain, config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: parsedQuery.data.grain, vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });

    const result = await service.getVaultApiRequests({
      from: range.from,
      grain: parsedQuery.data.grain,
      to: range.to,
      vaultId: params.data.vaultId,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: parsedQuery.data.grain,
      items: result.items,
      query: result.query,
      to: range.to.toISOString(),
      vault_id: params.data.vaultId,
      workspace_id: params.data.workspaceId
    };
  });

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/vaults/:vaultId/model-usage`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseVaultParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace or vault id');
    if (!hasWorkspaceAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    if (!hasVaultAccess(request, params.data.vaultId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });
    const parsedQuery = baseQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, parsedQuery.data.grain, config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: parsedQuery.data.grain, vault_id: params.data.vaultId, workspace_id: params.data.workspaceId });

    const result = await service.getModelUsage({
      from: range.from,
      grain: parsedQuery.data.grain,
      to: range.to,
      vaultId: params.data.vaultId,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: parsedQuery.data.grain,
      items: result.items,
      query: result.query,
      to: range.to.toISOString(),
      vault_id: params.data.vaultId,
      workspace_id: params.data.workspaceId
    };
  });

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/api-requests`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseWorkspaceParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace id');
    if (!hasWorkspaceRollupAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    const parsedQuery = baseQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, parsedQuery.data.grain, config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: parsedQuery.data.grain, workspace_id: params.data.workspaceId });

    const result = await service.getApiRequests({
      from: range.from,
      grain: parsedQuery.data.grain,
      to: range.to,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: parsedQuery.data.grain,
      items: result.items,
      query: result.query,
      to: range.to.toISOString(),
      workspace_id: params.data.workspaceId
    };
  });

  app.get(`${analyticsRoutePrefix}/v1/workspaces/:workspaceId/top-vaults`, { preHandler: analyticsAuth }, async (request, reply) => {
    const params = parseWorkspaceParams(request.params);
    if (!params.ok) return denyAnalyticsQuery(request, reply, 400, 'Invalid workspace id');
    if (!hasWorkspaceRollupAccess(request, params.data.workspaceId)) return denyAnalyticsQuery(request, reply, 403, 'Forbidden', { workspace_id: params.data.workspaceId });
    const parsedQuery = topVaultQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) return denyAnalyticsQuery(request, reply, 400, 'Invalid query', { workspace_id: params.data.workspaceId });
    const range = validateRange(parsedQuery.data.from, parsedQuery.data.to, 'day', config);
    if (!range.ok) return denyAnalyticsQuery(request, reply, 400, range.error, { grain: 'day', workspace_id: params.data.workspaceId });

    const result = await service.getTopVaults({
      from: range.from,
      limit: parsedQuery.data.limit,
      metric: parsedQuery.data.metric,
      to: range.to,
      workspaceId: params.data.workspaceId
    });

    return {
      from: range.from.toISOString(),
      grain: 'day',
      items: result.items,
      limit: parsedQuery.data.limit,
      metric: parsedQuery.data.metric,
      query: result.query,
      to: range.to.toISOString(),
      workspace_id: params.data.workspaceId
    };
  });
}

function denyAnalyticsQuery(
  request: FastifyRequest,
  reply: FastifyReply,
  statusCode: 400 | 403,
  error: string,
  details: Record<string, unknown> = {}
) {
  request.log.warn({
    ...details,
    error,
    route: request.routeOptions.url ?? request.url,
    status_code: statusCode
  }, 'Customer analytics query denied');
  return reply.code(statusCode).send({ error });
}

function parseWorkspaceParams(params: unknown): ParsedParams<{ workspaceId: string }> {
  const parsed = workspaceParamsSchema.safeParse(params);
  return parsed.success ? { data: parsed.data, ok: true } : { ok: false };
}

function parseVaultParams(params: unknown): ParsedParams<{ vaultId: string; workspaceId: string }> {
  const parsed = vaultParamsSchema.safeParse(params);
  return parsed.success ? { data: parsed.data, ok: true } : { ok: false };
}

function hasWorkspaceAccess(request: FastifyRequest, workspaceId: string): boolean {
  return request.auth.method === 'oauth' && request.auth.account_id === workspaceId;
}

function hasWorkspaceRollupAccess(request: FastifyRequest, workspaceId: string): boolean {
  return hasWorkspaceAccess(request, workspaceId) && !request.auth.vault_id;
}

function hasVaultAccess(request: FastifyRequest, vaultId: string): boolean {
  return !request.auth.vault_id || request.auth.vault_id === vaultId;
}

function validateRange(
  fromRaw: string,
  toRaw: string,
  grain: AnalyticsGrain,
  config: AppConfig
): { from: Date; ok: true; to: Date } | { error: string; ok: false } {
  const from = new Date(fromRaw);
  const to = new Date(toRaw);
  if (!Number.isFinite(from.getTime()) || !Number.isFinite(to.getTime())) {
    return { error: 'Invalid date range', ok: false };
  }
  if (from >= to) {
    return { error: '`from` must be before `to`', ok: false };
  }
  if (grain === 'day' && (!isUtcDayBoundary(from) || !isUtcDayBoundary(to))) {
    return { error: 'day queries must use UTC day boundaries', ok: false };
  }
  if (grain === 'hour' && (!isUtcHourBoundary(from) || !isUtcHourBoundary(to))) {
    return { error: 'hour queries must use UTC hour boundaries', ok: false };
  }

  const maxDays = grain === 'hour'
    ? config.ANALYTICS_HOURLY_MAX_RANGE_DAYS
    : config.ANALYTICS_DAILY_MAX_RANGE_DAYS;
  const maxMs = maxDays * 24 * 60 * 60 * 1000;
  if (to.getTime() - from.getTime() > maxMs) {
    return { error: `${grain} queries are limited to ${maxDays} days`, ok: false };
  }

  return { from, ok: true, to };
}

function isUtcDayBoundary(value: Date): boolean {
  return value.getUTCHours() === 0
    && value.getUTCMinutes() === 0
    && value.getUTCSeconds() === 0
    && value.getUTCMilliseconds() === 0;
}

function isUtcHourBoundary(value: Date): boolean {
  return value.getUTCMinutes() === 0
    && value.getUTCSeconds() === 0
    && value.getUTCMilliseconds() === 0;
}
