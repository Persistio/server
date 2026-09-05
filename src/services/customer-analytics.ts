import type { AppConfig } from '../config';
import {
  createAnalyticsSnapshotCache,
  type AnalyticsSnapshotCache,
  type SnapshotResult
} from './analytics-snapshot-cache';

export type AnalyticsGrain = 'hour' | 'day';
export type TopVaultMetric = 'api_requests' | 'errors' | 'rate_limited' | 'searches' | 'ingest_events' | 'memory_adds' | 'memory_count';

interface BigQueryJobLike {
  id?: string;
  metadata?: {
    id?: string;
  };
  getMetadata(): Promise<[{
    id?: string;
    statistics?: {
      query?: {
        totalBytesBilled?: string;
        totalBytesProcessed?: string;
      };
    };
  }]>;
  getQueryResults(): Promise<[Record<string, unknown>[]]>;
}

interface BigQueryClientLike {
  createQueryJob(options: {
    jobPrefix?: string;
    labels?: Record<string, string>;
    location?: string;
    maximumBytesBilled?: string;
    params?: Record<string, number | string>;
    query: string;
    types?: Record<string, string>;
  }): Promise<[BigQueryJobLike]>;
}

export interface CustomerAnalyticsQueryInfo {
  cache_hit?: boolean;
  cache_refreshed_at?: string;
  job_id?: string;
  job_bytes_billed: number | null;
  job_bytes_processed: number | null;
}

export interface CustomerAnalyticsLogger {
  error?(details: unknown, message?: string): void;
  info?(details: unknown, message?: string): void;
  warn?(details: unknown, message?: string): void;
}

interface BaseQueryInput {
  from: Date;
  grain: AnalyticsGrain;
  to: Date;
  workspaceId: string;
}

export interface WorkspaceMetricPoint {
  api_error_count: number;
  api_rate_limited_count: number;
  api_request_count: number;
  bucket_ts: string;
  completion_tokens: number;
  duration_p95_ms: number | null;
  embedding_input_chars: number;
  embedding_input_tokens: number;
  ingest_events_delta: number;
  memory_adds_delta: number;
  memory_count_delta: number;
  model_request_count: number;
  prompt_tokens: number;
  searches_delta: number;
  storage_bytes_delta: number;
  total_tokens: number;
}

export interface ApiRequestMetricPoint {
  api_error_count: number;
  api_rate_limited_count: number;
  api_request_count: number;
  bucket_ts: string;
  duration_p95_ms: number | null;
  method: string | null;
  operation: string | null;
  route: string | null;
  status_code: number | null;
}

export interface ModelUsageMetricPoint {
  completion_tokens: number;
  embedding_input_chars: number;
  embedding_input_tokens: number;
  model: string | null;
  model_request_count: number;
  model_role: string | null;
  prompt_tokens: number;
  provider: string | null;
  total_tokens: number;
}

export interface TopVaultMetricPoint {
  api_error_count: number;
  api_rate_limited_count: number;
  api_request_count: number;
  ingest_events_delta: number;
  memory_adds_delta: number;
  memory_count_delta: number;
  metric_value: number;
  searches_delta: number;
  vault_id: string;
}

export class CustomerAnalyticsService {
  private readonly client: BigQueryClientLike;
  private readonly location: string;
  private readonly maximumBytesBilled: string;
  private readonly projectId: string;
  private readonly rollupDataset: string;
  private readonly snapshotCache: AnalyticsSnapshotCache | null;

  constructor(
    config: AppConfig,
    client?: BigQueryClientLike,
    snapshotCache?: AnalyticsSnapshotCache | null,
    private readonly logger?: CustomerAnalyticsLogger
  ) {
    this.projectId = sanitizeProjectIdentifier(config.ANALYTICS_BIGQUERY_PROJECT_ID || config.GCP_PUBSUB_PROJECT_ID || config.VERTEX_PROJECT_ID);
    this.rollupDataset = sanitizeDatasetIdentifier(config.ANALYTICS_BIGQUERY_ROLLUP_DATASET);
    this.location = config.ANALYTICS_BIGQUERY_LOCATION;
    this.maximumBytesBilled = String(config.ANALYTICS_BIGQUERY_MAXIMUM_BYTES_BILLED);
    this.client = client ?? createBigQueryClient(this.projectId);
    this.snapshotCache = snapshotCache === undefined ? createAnalyticsSnapshotCache(config) : snapshotCache;
  }

  async getWorkspaceSummary(input: BaseQueryInput): Promise<{ items: WorkspaceMetricPoint[]; query: CustomerAnalyticsQueryInfo }> {
    const cached = await this.tryGetWorkspaceSummarySnapshot(input);
    if (cached) return cached;

    // The App dashboard is a vault activity summary, so derive it from the same
    // per-vault rollups that power vault metrics and top-vault rankings.
    const table = this.tableName(input.grain, 'vault');
    const result = await this.queryRows({
      labels: this.labels('workspace_metrics_summary', input),
      params: params(input),
      query: `
        SELECT
          bucket_ts,
          SUM(api_request_count) AS api_request_count,
          SUM(api_error_count) AS api_error_count,
          SUM(api_rate_limited_count) AS api_rate_limited_count,
          SUM(searches_delta) AS searches_delta,
          SUM(ingest_events_delta) AS ingest_events_delta,
          SUM(memory_adds_delta) AS memory_adds_delta,
          SUM(memory_count_delta) AS memory_count_delta,
          SUM(model_request_count) AS model_request_count,
          SUM(prompt_tokens) AS prompt_tokens,
          SUM(completion_tokens) AS completion_tokens,
          SUM(total_tokens) AS total_tokens,
          SUM(embedding_input_tokens) AS embedding_input_tokens,
          SUM(embedding_input_chars) AS embedding_input_chars,
          SUM(storage_bytes_delta) AS storage_bytes_delta,
          CAST(NULL AS FLOAT64) AS duration_p95_ms
        FROM \`${table}\`
        WHERE workspace_id = @workspaceId
          AND bucket_date BETWEEN DATE(TIMESTAMP(@fromTs)) AND DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))
          AND bucket_ts >= TIMESTAMP(@fromTs)
          AND bucket_ts < TIMESTAMP(@toTs)
        GROUP BY bucket_ts
        ORDER BY bucket_ts ASC`
    });

    const response = {
      items: result.rows.map(rowToWorkspaceMetricPoint),
      query: result.query
    };
    await this.trySetWorkspaceSummarySnapshot(input, response);
    return response;
  }

  async getVaultMetrics(input: BaseQueryInput & { vaultId: string }): Promise<{ items: WorkspaceMetricPoint[]; query: CustomerAnalyticsQueryInfo }> {
    const cached = await this.tryGetVaultMetricsSnapshot(input);
    if (cached) return cached;

    const table = this.tableName(input.grain, 'vault');
    const result = await this.queryRows({
      labels: this.labels('vault_metrics', input),
      params: {
        ...params(input),
        vaultId: input.vaultId
      },
      query: `
        SELECT
          bucket_ts,
          SUM(api_request_count) AS api_request_count,
          SUM(api_error_count) AS api_error_count,
          SUM(api_rate_limited_count) AS api_rate_limited_count,
          SUM(searches_delta) AS searches_delta,
          SUM(ingest_events_delta) AS ingest_events_delta,
          SUM(memory_adds_delta) AS memory_adds_delta,
          SUM(memory_count_delta) AS memory_count_delta,
          SUM(model_request_count) AS model_request_count,
          SUM(prompt_tokens) AS prompt_tokens,
          SUM(completion_tokens) AS completion_tokens,
          SUM(total_tokens) AS total_tokens,
          SUM(embedding_input_tokens) AS embedding_input_tokens,
          SUM(embedding_input_chars) AS embedding_input_chars,
          SUM(storage_bytes_delta) AS storage_bytes_delta,
          CAST(NULL AS FLOAT64) AS duration_p95_ms
        FROM \`${table}\`
        WHERE workspace_id = @workspaceId
          AND vault_id = @vaultId
          AND bucket_date BETWEEN DATE(TIMESTAMP(@fromTs)) AND DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))
          AND bucket_ts >= TIMESTAMP(@fromTs)
          AND bucket_ts < TIMESTAMP(@toTs)
        GROUP BY bucket_ts
        ORDER BY bucket_ts ASC`
    });

    const response = {
      items: result.rows.map(rowToWorkspaceMetricPoint),
      query: result.query
    };
    await this.trySetVaultMetricsSnapshot(input, response);
    return response;
  }

  async getApiRequests(input: BaseQueryInput): Promise<{ items: ApiRequestMetricPoint[]; query: CustomerAnalyticsQueryInfo }> {
    const table = this.tableName(input.grain, 'workspace');
    const result = await this.queryRows({
      labels: this.labels('workspace_api_requests', input),
      params: params(input),
      query: `
        SELECT
          MAX(bucket_ts) AS bucket_ts,
          operation,
          route,
          method,
          status_code,
          SUM(api_request_count) AS api_request_count,
          SUM(api_error_count) AS api_error_count,
          SUM(api_rate_limited_count) AS api_rate_limited_count,
          CAST(NULL AS FLOAT64) AS duration_p95_ms
        FROM \`${table}\`
        WHERE workspace_id = @workspaceId
          AND metric_family = 'api_request'
          AND bucket_date BETWEEN DATE(TIMESTAMP(@fromTs)) AND DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))
          AND bucket_ts >= TIMESTAMP(@fromTs)
          AND bucket_ts < TIMESTAMP(@toTs)
        GROUP BY operation, route, method, status_code
        ORDER BY api_request_count DESC, route ASC`
    });

    return {
      items: result.rows.map(rowToApiRequestMetricPoint),
      query: result.query
    };
  }

  async getVaultApiRequests(input: BaseQueryInput & { vaultId: string }): Promise<{ items: ApiRequestMetricPoint[]; query: CustomerAnalyticsQueryInfo }> {
    const table = this.tableName(input.grain, 'vault');
    const result = await this.queryRows({
      labels: this.labels('vault_api_requests', input),
      params: {
        ...params(input),
        vaultId: input.vaultId
      },
      query: `
        SELECT
          MAX(bucket_ts) AS bucket_ts,
          operation,
          route,
          method,
          status_code,
          SUM(api_request_count) AS api_request_count,
          SUM(api_error_count) AS api_error_count,
          SUM(api_rate_limited_count) AS api_rate_limited_count,
          CAST(NULL AS FLOAT64) AS duration_p95_ms
        FROM \`${table}\`
        WHERE workspace_id = @workspaceId
          AND vault_id = @vaultId
          AND metric_family = 'api_request'
          AND bucket_date BETWEEN DATE(TIMESTAMP(@fromTs)) AND DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))
          AND bucket_ts >= TIMESTAMP(@fromTs)
          AND bucket_ts < TIMESTAMP(@toTs)
        GROUP BY operation, route, method, status_code
        ORDER BY api_request_count DESC, route ASC`
    });

    return {
      items: result.rows.map(rowToApiRequestMetricPoint),
      query: result.query
    };
  }

  async getModelUsage(input: BaseQueryInput & { vaultId?: string }): Promise<{ items: ModelUsageMetricPoint[]; query: CustomerAnalyticsQueryInfo }> {
    const scope = input.vaultId ? 'vault' : 'workspace';
    const table = this.tableName(input.grain, scope);
    const result = await this.queryRows({
      labels: this.labels(input.vaultId ? 'vault_model_usage' : 'workspace_model_usage', input),
      params: {
        ...params(input),
        ...(input.vaultId ? { vaultId: input.vaultId } : {})
      },
      query: `
        SELECT
          model_role,
          provider,
          model,
          SUM(model_request_count) AS model_request_count,
          SUM(prompt_tokens) AS prompt_tokens,
          SUM(completion_tokens) AS completion_tokens,
          SUM(total_tokens) AS total_tokens,
          SUM(embedding_input_tokens) AS embedding_input_tokens,
          SUM(embedding_input_chars) AS embedding_input_chars
        FROM \`${table}\`
        WHERE workspace_id = @workspaceId
          ${input.vaultId ? 'AND vault_id = @vaultId' : ''}
          AND metric_family = 'model_usage'
          AND bucket_date BETWEEN DATE(TIMESTAMP(@fromTs)) AND DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))
          AND bucket_ts >= TIMESTAMP(@fromTs)
          AND bucket_ts < TIMESTAMP(@toTs)
        GROUP BY model_role, provider, model
        ORDER BY model_request_count DESC, total_tokens DESC, model_role ASC`
    });

    return {
      items: result.rows.map(rowToModelUsageMetricPoint),
      query: result.query
    };
  }

  async getTopVaults(input: Omit<BaseQueryInput, 'grain'> & { limit: number; metric: TopVaultMetric }): Promise<{ items: TopVaultMetricPoint[]; query: CustomerAnalyticsQueryInfo }> {
    const cached = await this.tryGetTopVaultsSnapshot(input);
    if (cached) return cached;

    const table = this.tableName('day', 'vault');
    const metricExpression = topVaultMetricExpression(input.metric);
    const result = await this.queryRows({
      labels: this.labels('workspace_top_vaults', { ...input, grain: 'day' }),
      params: {
        ...params({ ...input, grain: 'day' }),
        limit: input.limit
      },
      query: `
        SELECT
          vault_id,
          SUM(api_request_count) AS api_request_count,
          SUM(api_error_count) AS api_error_count,
          SUM(api_rate_limited_count) AS api_rate_limited_count,
          SUM(searches_delta) AS searches_delta,
          SUM(ingest_events_delta) AS ingest_events_delta,
          SUM(memory_adds_delta) AS memory_adds_delta,
          SUM(memory_count_delta) AS memory_count_delta,
          ${metricExpression} AS metric_value
        FROM \`${table}\`
        WHERE workspace_id = @workspaceId
          AND bucket_date BETWEEN DATE(TIMESTAMP(@fromTs)) AND DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))
          AND bucket_ts >= TIMESTAMP(@fromTs)
          AND bucket_ts < TIMESTAMP(@toTs)
        GROUP BY vault_id
        HAVING metric_value != 0
        ORDER BY metric_value DESC, vault_id ASC
        LIMIT @limit`
    });

    const response = {
      items: result.rows.map(rowToTopVaultMetricPoint),
      query: result.query
    };
    await this.trySetTopVaultsSnapshot(input, response);
    return response;
  }

  private async tryGetWorkspaceSummarySnapshot(input: BaseQueryInput): Promise<SnapshotResult<WorkspaceMetricPoint> | null> {
    if (!this.snapshotCache) return null;
    try {
      return await this.snapshotCache.getWorkspaceSummary(input);
    } catch (error) {
      console.warn('Failed to read workspace analytics snapshot cache', { error });
      return null;
    }
  }

  private async trySetWorkspaceSummarySnapshot(
    input: BaseQueryInput,
    result: SnapshotResult<WorkspaceMetricPoint>
  ): Promise<void> {
    if (!this.snapshotCache) return;
    try {
      await this.snapshotCache.setWorkspaceSummary(input, result);
    } catch (error) {
      console.warn('Failed to write workspace analytics snapshot cache', { error });
    }
  }

  private async tryGetVaultMetricsSnapshot(input: BaseQueryInput & { vaultId: string }): Promise<SnapshotResult<WorkspaceMetricPoint> | null> {
    if (!this.snapshotCache) return null;
    try {
      return await this.snapshotCache.getVaultMetrics(input);
    } catch (error) {
      console.warn('Failed to read vault analytics snapshot cache', { error });
      return null;
    }
  }

  private async trySetVaultMetricsSnapshot(
    input: BaseQueryInput & { vaultId: string },
    result: SnapshotResult<WorkspaceMetricPoint>
  ): Promise<void> {
    if (!this.snapshotCache) return;
    try {
      await this.snapshotCache.setVaultMetrics(input, result);
    } catch (error) {
      console.warn('Failed to write vault analytics snapshot cache', { error });
    }
  }

  private async tryGetTopVaultsSnapshot(input: Omit<BaseQueryInput, 'grain'> & { limit: number; metric: TopVaultMetric }): Promise<SnapshotResult<TopVaultMetricPoint> | null> {
    if (!this.snapshotCache) return null;
    try {
      return await this.snapshotCache.getTopVaults(input);
    } catch (error) {
      console.warn('Failed to read top-vault analytics snapshot cache', { error });
      return null;
    }
  }

  private async trySetTopVaultsSnapshot(
    input: Omit<BaseQueryInput, 'grain'> & { limit: number; metric: TopVaultMetric },
    result: SnapshotResult<TopVaultMetricPoint>
  ): Promise<void> {
    if (!this.snapshotCache) return;
    try {
      await this.snapshotCache.setTopVaults(input, result);
    } catch (error) {
      console.warn('Failed to write top-vault analytics snapshot cache', { error });
    }
  }

  private tableName(grain: AnalyticsGrain, scope: 'workspace' | 'vault'): string {
    const suffix = grain === 'day' ? 'daily' : 'hourly';
    return `${this.projectId}.${this.rollupDataset}.${scope}_usage_${suffix}`;
  }

  private labels(route: string, input: BaseQueryInput): Record<string, string> {
    return {
      feature: 'customer_metrics',
      grain: input.grain,
      route,
      workspace_id: input.workspaceId.toLowerCase()
    };
  }

  private async queryRows(input: {
    labels: Record<string, string>;
    params: Record<string, number | string>;
    query: string;
  }): Promise<{ rows: Record<string, unknown>[]; query: CustomerAnalyticsQueryInfo }> {
    let job: BigQueryJobLike;
    const startedAt = Date.now();
    try {
      [job] = await this.client.createQueryJob({
        jobPrefix: 'persistio_analytics_',
        labels: input.labels,
        location: this.location,
        maximumBytesBilled: this.maximumBytesBilled,
        params: input.params,
        query: input.query,
        types: Object.fromEntries(Object.entries(input.params).map(([key, value]) => [
          key,
          typeof value === 'number' ? 'INT64' : 'STRING'
        ]))
      });
    } catch (error) {
      this.logAnalyticsQueryError(error, input.labels, null);
      throw error;
    }

    const jobId = bigQueryJobId(job);
    try {
      const [rows] = await job.getQueryResults();
      const [metadata] = await job.getMetadata();
      const completedJobId = bigQueryJobId(job, metadata);
      const bytesBilled = toNullableNumber(metadata.statistics?.query?.totalBytesBilled);
      const bytesProcessed = toNullableNumber(metadata.statistics?.query?.totalBytesProcessed);
      this.logger?.info?.({
        bytes_billed: bytesBilled,
        bytes_processed: bytesProcessed,
        duration_ms: Date.now() - startedAt,
        job_id: completedJobId,
        labels: input.labels,
        maximum_bytes_billed: this.maximumBytesBilled
      }, 'Customer analytics BigQuery query completed');

      return {
        rows,
        query: {
          ...(completedJobId ? { job_id: completedJobId } : {}),
          job_bytes_billed: bytesBilled,
          job_bytes_processed: bytesProcessed
        }
      };
    } catch (error) {
      this.logAnalyticsQueryError(error, input.labels, jobId ?? null);
      throw error;
    }
  }

  private logAnalyticsQueryError(error: unknown, labels: Record<string, string>, jobId: string | null): void {
    const details = {
      error: errorMessage(error),
      job_id: jobId,
      labels,
      maximum_bytes_billed: this.maximumBytesBilled
    };
    if (isBigQueryDeniedError(error)) {
      this.logger?.warn?.(details, 'Customer analytics BigQuery query denied');
      return;
    }
    this.logger?.error?.(details, 'Customer analytics BigQuery query failed');
  }
}

function createBigQueryClient(projectId: string): BigQueryClientLike {
  // Loaded lazily so non-analytics roles do not initialize BigQuery clients.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { BigQuery } = require('@google-cloud/bigquery') as typeof import('@google-cloud/bigquery');
  return new BigQuery({ projectId }) as unknown as BigQueryClientLike;
}

function bigQueryJobId(job: BigQueryJobLike, metadata?: { id?: string }): string | undefined {
  return metadata?.id ?? job.metadata?.id ?? job.id;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isBigQueryDeniedError(error: unknown): boolean {
  const record = error as { code?: unknown; errors?: Array<{ reason?: string }>; statusCode?: unknown };
  const code = typeof record?.code === 'number' ? record.code : Number(record?.code);
  const statusCode = typeof record?.statusCode === 'number' ? record.statusCode : Number(record?.statusCode);
  const reasons = Array.isArray(record?.errors)
    ? record.errors.map((item) => item.reason)
    : [];
  return code === 403
    || statusCode === 403
    || reasons.some((reason) => reason === 'accessDenied' || reason === 'billingTierLimitExceeded' || reason === 'quotaExceeded');
}

function params(input: BaseQueryInput): Record<string, string> {
  return {
    fromTs: input.from.toISOString(),
    toTs: input.to.toISOString(),
    workspaceId: input.workspaceId
  };
}

function sanitizeProjectIdentifier(value: string): string {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error('ANALYTICS_BIGQUERY_PROJECT_ID must contain only letters, numbers, underscores, or dashes');
  }
  return value;
}

function sanitizeDatasetIdentifier(value: string): string {
  if (!/^[A-Za-z0-9_]+$/.test(value)) {
    throw new Error('ANALYTICS_BIGQUERY_ROLLUP_DATASET must contain only letters, numbers, or underscores');
  }
  return value;
}

function topVaultMetricExpression(metric: TopVaultMetric): string {
  switch (metric) {
    case 'api_requests':
      return 'SUM(api_request_count)';
    case 'errors':
      return 'SUM(api_error_count)';
    case 'rate_limited':
      return 'SUM(api_rate_limited_count)';
    case 'searches':
      return 'SUM(searches_delta)';
    case 'ingest_events':
      return 'SUM(ingest_events_delta)';
    case 'memory_adds':
      return 'SUM(memory_adds_delta)';
    case 'memory_count':
      return 'SUM(memory_count_delta)';
  }
}

function rowToWorkspaceMetricPoint(row: Record<string, unknown>): WorkspaceMetricPoint {
  return {
    api_error_count: toNumber(row.api_error_count),
    api_rate_limited_count: toNumber(row.api_rate_limited_count),
    api_request_count: toNumber(row.api_request_count),
    bucket_ts: toTimestampString(row.bucket_ts),
    completion_tokens: toNumber(row.completion_tokens),
    duration_p95_ms: toNullableNumber(row.duration_p95_ms),
    embedding_input_chars: toNumber(row.embedding_input_chars),
    embedding_input_tokens: toNumber(row.embedding_input_tokens),
    ingest_events_delta: toNumber(row.ingest_events_delta),
    memory_adds_delta: toNumber(row.memory_adds_delta),
    memory_count_delta: toNumber(row.memory_count_delta),
    model_request_count: toNumber(row.model_request_count),
    prompt_tokens: toNumber(row.prompt_tokens),
    searches_delta: toNumber(row.searches_delta),
    storage_bytes_delta: toNumber(row.storage_bytes_delta),
    total_tokens: toNumber(row.total_tokens)
  };
}

function rowToApiRequestMetricPoint(row: Record<string, unknown>): ApiRequestMetricPoint {
  return {
    api_error_count: toNumber(row.api_error_count),
    api_rate_limited_count: toNumber(row.api_rate_limited_count),
    api_request_count: toNumber(row.api_request_count),
    bucket_ts: toTimestampString(row.bucket_ts),
    duration_p95_ms: toNullableNumber(row.duration_p95_ms),
    method: toNullableString(row.method),
    operation: toNullableString(row.operation),
    route: toNullableString(row.route),
    status_code: toNullableNumber(row.status_code)
  };
}

function rowToModelUsageMetricPoint(row: Record<string, unknown>): ModelUsageMetricPoint {
  return {
    completion_tokens: toNumber(row.completion_tokens),
    embedding_input_chars: toNumber(row.embedding_input_chars),
    embedding_input_tokens: toNumber(row.embedding_input_tokens),
    model: toNullableString(row.model),
    model_request_count: toNumber(row.model_request_count),
    model_role: toNullableString(row.model_role),
    prompt_tokens: toNumber(row.prompt_tokens),
    provider: toNullableString(row.provider),
    total_tokens: toNumber(row.total_tokens)
  };
}

function rowToTopVaultMetricPoint(row: Record<string, unknown>): TopVaultMetricPoint {
  return {
    api_error_count: toNumber(row.api_error_count),
    api_rate_limited_count: toNumber(row.api_rate_limited_count),
    api_request_count: toNumber(row.api_request_count),
    ingest_events_delta: toNumber(row.ingest_events_delta),
    memory_adds_delta: toNumber(row.memory_adds_delta),
    memory_count_delta: toNumber(row.memory_count_delta),
    metric_value: toNumber(row.metric_value),
    searches_delta: toNumber(row.searches_delta),
    vault_id: String(row.vault_id)
  };
}

function toNumber(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') return Number(value);
  return 0;
}

function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  return toNumber(value);
}

function toNullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function toTimestampString(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object' && value !== null && 'value' in value) {
    return String((value as { value: unknown }).value);
  }
  return String(value);
}
