import { describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../config';
import {
  FirestoreAnalyticsSnapshotCache,
  type AnalyticsSnapshotCache
} from './analytics-snapshot-cache';
import { CustomerAnalyticsService } from './customer-analytics';

function appConfig(overrides: Partial<AppConfig> = {}): AppConfig {
  return {
    ANALYTICS_BIGQUERY_LOCATION: 'EU',
    ANALYTICS_BIGQUERY_MAXIMUM_BYTES_BILLED: 12345,
    ANALYTICS_BIGQUERY_PROJECT_ID: 'persistio',
    ANALYTICS_BIGQUERY_ROLLUP_DATASET: 'persistio_analytics_rollup',
    GCP_PUBSUB_PROJECT_ID: '',
    VERTEX_PROJECT_ID: '',
    ...overrides
  } as AppConfig;
}

function bigQueryClient(rows: Record<string, unknown>[] = []) {
  const job = {
    getMetadata: vi.fn().mockResolvedValue([{
      statistics: {
        query: {
          totalBytesBilled: '42',
          totalBytesProcessed: '128'
        }
      }
    }]),
    getQueryResults: vi.fn().mockResolvedValue([rows])
  };
  const client = {
    createQueryJob: vi.fn().mockResolvedValue([job])
  };
  return { client, job };
}

function analyticsInput() {
  return {
    from: new Date('2026-06-11T00:00:00.000Z'),
    grain: 'hour' as const,
    to: new Date('2026-06-12T00:00:00.000Z'),
    workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
  };
}

function snapshotCache(overrides: Partial<AnalyticsSnapshotCache> = {}): AnalyticsSnapshotCache {
  return {
    getTopVaults: vi.fn().mockResolvedValue(null),
    getVaultMetrics: vi.fn().mockResolvedValue(null),
    getWorkspaceSummary: vi.fn().mockResolvedValue(null),
    setTopVaults: vi.fn().mockResolvedValue(undefined),
    setVaultMetrics: vi.fn().mockResolvedValue(undefined),
    setWorkspaceSummary: vi.fn().mockResolvedValue(undefined),
    ...overrides
  };
}

function firestoreStore(initial: Record<string, unknown> = {}) {
  const docs = new Map(Object.entries(initial));
  return {
    docs,
    firestore: {
      collection: vi.fn(() => ({
        doc: vi.fn((id: string) => ({
          get: vi.fn(async () => ({
            exists: docs.has(id),
            data: () => docs.get(id) as Record<string, unknown> | undefined
          })),
          set: vi.fn(async (data: Record<string, unknown>) => {
            docs.set(id, data);
          })
        }))
      }))
    }
  };
}

describe('CustomerAnalyticsService', () => {
  it('queries workspace summary from vault rollups with job guardrails', async () => {
    const { client } = bigQueryClient([{
      api_error_count: '1',
      api_rate_limited_count: '0',
      api_request_count: '10',
      bucket_ts: { value: '2026-06-11T09:00:00.000Z' },
      searches_delta: '3'
    }]);
    const service = new CustomerAnalyticsService(appConfig(), client);

    const result = await service.getWorkspaceSummary({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob).toHaveBeenCalledWith(expect.objectContaining({
      labels: {
        feature: 'customer_metrics',
        grain: 'hour',
        route: 'workspace_metrics_summary',
        workspace_id: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
      },
      location: 'EU',
      maximumBytesBilled: '12345',
      params: {
        fromTs: '2026-06-11T00:00:00.000Z',
        toTs: '2026-06-12T00:00:00.000Z',
        workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
      }
    }));
    expect(client.createQueryJob.mock.calls[0][0].query).toContain('persistio.persistio_analytics_rollup.vault_usage_hourly');
    expect(client.createQueryJob.mock.calls[0][0].query).not.toContain('vault_id = @vaultId');
    expect(client.createQueryJob.mock.calls[0][0].query).toContain('CAST(NULL AS FLOAT64) AS duration_p95_ms');
    expect(client.createQueryJob.mock.calls[0][0].query).not.toContain('MAX(duration_p95_ms) AS duration_p95_ms');
    expect(result).toMatchObject({
      items: [{ api_error_count: 1, api_request_count: 10, searches_delta: 3 }],
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });
  });

  it('serves default workspace summaries from the snapshot cache without BigQuery', async () => {
    const { client } = bigQueryClient();
    const cache = snapshotCache({
      getWorkspaceSummary: vi.fn().mockResolvedValue({
        items: [{ api_error_count: 0, api_rate_limited_count: 0, api_request_count: 25, bucket_ts: '2026-05-12T00:00:00.000Z' }],
        query: { cache_hit: true, job_bytes_billed: 0, job_bytes_processed: 0 }
      })
    });
    const service = new CustomerAnalyticsService(appConfig(), client, cache);

    const result = await service.getWorkspaceSummary({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      items: [{ api_request_count: 25 }],
      query: { cache_hit: true }
    });
  });

  it('reseeds the workspace snapshot cache from BigQuery on miss', async () => {
    const { client } = bigQueryClient([{
      api_error_count: '0',
      api_rate_limited_count: '0',
      api_request_count: '10',
      bucket_ts: { value: '2026-06-10T00:00:00.000Z' }
    }]);
    const cache = snapshotCache();
    const service = new CustomerAnalyticsService(appConfig(), client, cache);

    const result = await service.getWorkspaceSummary({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob).toHaveBeenCalledOnce();
    expect(cache.setWorkspaceSummary).toHaveBeenCalledWith(expect.any(Object), result);
  });

  it('does not merge per-dimension latency percentiles into vault summaries', async () => {
    const { client } = bigQueryClient();
    const service = new CustomerAnalyticsService(appConfig(), client);

    await service.getVaultMetrics({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    const query = client.createQueryJob.mock.calls[0][0].query;
    expect(query).toContain('CAST(NULL AS FLOAT64) AS duration_p95_ms');
    expect(query).not.toContain('MAX(duration_p95_ms) AS duration_p95_ms');
  });

  it('serves vault metrics from the snapshot cache without BigQuery', async () => {
    const { client } = bigQueryClient();
    const cache = snapshotCache({
      getVaultMetrics: vi.fn().mockResolvedValue({
        items: [{
          api_error_count: 0,
          api_rate_limited_count: 0,
          api_request_count: 12,
          bucket_ts: '2026-06-10T00:00:00.000Z'
        }],
        query: { cache_hit: true, job_bytes_billed: 0, job_bytes_processed: 0 }
      })
    });
    const service = new CustomerAnalyticsService(appConfig(), client, cache);

    const result = await service.getVaultMetrics({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      items: [{ api_request_count: 12 }],
      query: { cache_hit: true }
    });
  });

  it('reseeds the vault snapshot cache from BigQuery on miss', async () => {
    const { client } = bigQueryClient([{
      api_error_count: '0',
      api_rate_limited_count: '0',
      api_request_count: '10',
      bucket_ts: { value: '2026-06-10T00:00:00.000Z' }
    }]);
    const cache = snapshotCache();
    const service = new CustomerAnalyticsService(appConfig(), client, cache);

    const result = await service.getVaultMetrics({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob).toHaveBeenCalledOnce();
    expect(cache.setVaultMetrics).toHaveBeenCalledWith(expect.any(Object), result);
  });

  it('rejects unsafe configured BigQuery identifiers', () => {
    expect(() => new CustomerAnalyticsService(appConfig({
      ANALYTICS_BIGQUERY_ROLLUP_DATASET: 'rollup;DROP'
    }))).toThrow('ANALYTICS_BIGQUERY_ROLLUP_DATASET must contain only letters');
  });

  it('casts string timestamp params before filtering partition dates', async () => {
    const { client } = bigQueryClient();
    const service = new CustomerAnalyticsService(appConfig(), client);

    await service.getApiRequests({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    const query = client.createQueryJob.mock.calls[0][0].query;
    expect(query).toContain('bucket_date BETWEEN DATE(TIMESTAMP(@fromTs))');
    expect(query).toContain('DATE(TIMESTAMP_SUB(TIMESTAMP(@toTs), INTERVAL 1 MICROSECOND))');
  });

  it('aggregates workspace API request rows without merging percentile latency', async () => {
    const { client } = bigQueryClient([{
      api_error_count: '3',
      api_rate_limited_count: '1',
      api_request_count: '42',
      bucket_ts: { value: '2026-06-11T09:00:00.000Z' },
      duration_p95_ms: null,
      method: 'POST',
      route: '/v1/recall',
      status_code: '200'
    }]);
    const service = new CustomerAnalyticsService(appConfig(), client);

    const result = await service.getApiRequests({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    const query = client.createQueryJob.mock.calls[0][0].query;
    expect(query).toContain('CAST(NULL AS FLOAT64) AS duration_p95_ms');
    expect(query).not.toContain('MAX(duration_p95_ms) AS duration_p95_ms');
    expect(query).not.toContain('GROUP BY bucket_ts');
    expect(result.items).toEqual([
      expect.objectContaining({
        api_request_count: 42,
        duration_p95_ms: null,
        route: '/v1/recall'
      })
    ]);
  });

  it('aggregates vault API request rows without merging percentile latency', async () => {
    const { client } = bigQueryClient();
    const service = new CustomerAnalyticsService(appConfig(), client);

    await service.getVaultApiRequests({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    const query = client.createQueryJob.mock.calls[0][0].query;
    expect(query).toContain('CAST(NULL AS FLOAT64) AS duration_p95_ms');
    expect(query).not.toContain('MAX(duration_p95_ms) AS duration_p95_ms');
    expect(query).not.toContain('GROUP BY bucket_ts');
    expect(query).toContain('vault_id = @vaultId');
  });

  it('reads BigQuery job metadata after query results complete', async () => {
    const events: string[] = [];
    let resultsComplete = false;
    const job = {
      getMetadata: vi.fn().mockImplementation(async () => {
        events.push(resultsComplete ? 'metadata_after_results' : 'metadata_before_results');
        return [{
          statistics: {
            query: {
              totalBytesBilled: resultsComplete ? '42' : undefined,
              totalBytesProcessed: resultsComplete ? '128' : undefined
            }
          }
        }];
      }),
      getQueryResults: vi.fn().mockImplementation(async () => {
        events.push('results');
        resultsComplete = true;
        return [[]];
      })
    };
    const client = {
      createQueryJob: vi.fn().mockResolvedValue([job])
    };
    const service = new CustomerAnalyticsService(appConfig(), client);

    const result = await service.getApiRequests({
      from: new Date('2026-06-11T00:00:00.000Z'),
      grain: 'hour',
      to: new Date('2026-06-12T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(events).toEqual(['results', 'metadata_after_results']);
    expect(result.query).toEqual({
      job_bytes_billed: 42,
      job_bytes_processed: 128
    });
  });

  it('logs BigQuery job ids and byte counts for completed queries', async () => {
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const job = {
      id: 'persistio_analytics_job_1',
      getMetadata: vi.fn().mockResolvedValue([{
        id: 'persistio_analytics_job_1',
        statistics: {
          query: {
            totalBytesBilled: '42',
            totalBytesProcessed: '128'
          }
        }
      }]),
      getQueryResults: vi.fn().mockResolvedValue([[]])
    };
    const client = {
      createQueryJob: vi.fn().mockResolvedValue([job])
    };
    const service = new CustomerAnalyticsService(appConfig(), client, null, logger);

    const result = await service.getApiRequests(analyticsInput());

    expect(result.query).toEqual({
      job_bytes_billed: 42,
      job_bytes_processed: 128,
      job_id: 'persistio_analytics_job_1'
    });
    expect(logger.info).toHaveBeenCalledWith(expect.objectContaining({
      bytes_billed: 42,
      bytes_processed: 128,
      job_id: 'persistio_analytics_job_1',
      maximum_bytes_billed: '12345'
    }), 'Customer analytics BigQuery query completed');
    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('logs denied BigQuery queries as warnings', async () => {
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const denied = Object.assign(new Error('maximumBytesBilled exceeded'), { code: 403 });
    const client = {
      createQueryJob: vi.fn().mockRejectedValue(denied)
    };
    const service = new CustomerAnalyticsService(appConfig(), client, null, logger);

    await expect(service.getApiRequests(analyticsInput())).rejects.toThrow('maximumBytesBilled exceeded');

    expect(logger.warn).toHaveBeenCalledWith(expect.objectContaining({
      error: 'maximumBytesBilled exceeded',
      job_id: null,
      maximum_bytes_billed: '12345'
    }), 'Customer analytics BigQuery query denied');
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('logs BigQuery query failures with job context', async () => {
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const failed = Object.assign(new Error('backend unavailable'), { code: 500 });
    const job = {
      id: 'persistio_analytics_job_2',
      getMetadata: vi.fn(),
      getQueryResults: vi.fn().mockRejectedValue(failed)
    };
    const client = {
      createQueryJob: vi.fn().mockResolvedValue([job])
    };
    const service = new CustomerAnalyticsService(appConfig(), client, null, logger);

    await expect(service.getApiRequests(analyticsInput())).rejects.toThrow('backend unavailable');

    expect(logger.error).toHaveBeenCalledWith(expect.objectContaining({
      error: 'backend unavailable',
      job_id: 'persistio_analytics_job_2',
      maximum_bytes_billed: '12345'
    }), 'Customer analytics BigQuery query failed');
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('logs invalid BigQuery SQL responses as failures', async () => {
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const invalidQuery = Object.assign(new Error('Unrecognized name: storage_bytes_delta'), {
      code: 400,
      errors: [{ reason: 'invalidQuery' }]
    });
    const client = {
      createQueryJob: vi.fn().mockRejectedValue(invalidQuery)
    };
    const service = new CustomerAnalyticsService(appConfig(), client, null, logger);

    await expect(service.getApiRequests(analyticsInput())).rejects.toThrow('Unrecognized name');

    expect(logger.error).toHaveBeenCalledWith(expect.objectContaining({
      error: 'Unrecognized name: storage_bytes_delta',
      job_id: null
    }), 'Customer analytics BigQuery query failed');
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('uses the daily vault rollup for top vaults', async () => {
    const { client } = bigQueryClient();
    const service = new CustomerAnalyticsService(appConfig(), client);

    await service.getTopVaults({
      from: new Date('2026-06-01T00:00:00.000Z'),
      limit: 10,
      metric: 'searches',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob.mock.calls[0][0].query).toContain('persistio.persistio_analytics_rollup.vault_usage_daily');
    expect(client.createQueryJob).toHaveBeenCalledWith(expect.objectContaining({
      params: expect.objectContaining({ limit: 10 }),
      types: expect.objectContaining({ limit: 'INT64' })
    }));
    expect(client.createQueryJob.mock.calls[0][0].query).toContain('LIMIT @limit');
    expect(client.createQueryJob.mock.calls[0][0].query).not.toContain('LIMIT CAST(@limit AS INT64)');
  });

  it('serves default top vaults from the snapshot cache without BigQuery', async () => {
    const { client } = bigQueryClient();
    const cache = snapshotCache({
      getTopVaults: vi.fn().mockResolvedValue({
        items: [{
          api_error_count: 0,
          api_rate_limited_count: 0,
          api_request_count: 25,
          ingest_events_delta: 0,
          memory_adds_delta: 0,
          memory_count_delta: 0,
          metric_value: 25,
          searches_delta: 0,
          vault_id: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6'
        }],
        query: { cache_hit: true, job_bytes_billed: 0, job_bytes_processed: 0 }
      })
    });
    const service = new CustomerAnalyticsService(appConfig(), client, cache);

    const result = await service.getTopVaults({
      from: new Date('2026-05-12T00:00:00.000Z'),
      limit: 5,
      metric: 'api_requests',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(client.createQueryJob).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      items: [{ metric_value: 25 }],
      query: { cache_hit: true }
    });
  });
});

describe('FirestoreAnalyticsSnapshotCache', () => {
  it('stores and reads expiring default vault UI snapshots', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });
    const input = {
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day' as const,
      to: new Date('2026-06-11T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    };

    await cache.setVaultMetrics(input, {
      items: [{
        api_error_count: 0,
        api_rate_limited_count: 0,
        api_request_count: 10,
        bucket_ts: '2026-06-10T00:00:00.000Z'
      }] as never,
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    expect(store.docs.size).toBe(1);
    expect(Array.from(store.docs.keys())[0]).toContain('vault_metrics_default_30d');
    const doc = Array.from(store.docs.values())[0] as Record<string, unknown>;
    expect(doc).toMatchObject({
      grain: 'day',
      kind: 'vault_metrics_default_30d',
      vault_id: input.vaultId,
      workspace_id: input.workspaceId
    });
    expect(doc.expires_at).toBeInstanceOf(Date);

    const result = await cache.getVaultMetrics(input);
    expect(result).toMatchObject({
      items: [{ api_request_count: 10 }],
      query: { cache_hit: true, job_bytes_billed: 42, job_bytes_processed: 128 }
    });
  });

  it('overwrites one default vault UI snapshot document per vault', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });
    const input = {
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day' as const,
      to: new Date('2026-06-11T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    };

    await cache.setVaultMetrics(input, {
      items: [{ api_error_count: 0, api_rate_limited_count: 0, api_request_count: 10, bucket_ts: '2026-06-10T00:00:00.000Z' }] as never,
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });
    await cache.setVaultMetrics({
      ...input,
      from: new Date('2026-05-13T00:00:00.000Z'),
      to: new Date('2026-06-12T00:00:00.000Z')
    }, {
      items: [{ api_error_count: 0, api_rate_limited_count: 0, api_request_count: 25, bucket_ts: '2026-06-11T00:00:00.000Z' }] as never,
      query: { job_bytes_billed: 12, job_bytes_processed: 34 }
    });

    expect(store.docs.size).toBe(1);
    await expect(cache.getVaultMetrics(input)).resolves.toBeNull();
    await expect(cache.getVaultMetrics({
      ...input,
      from: new Date('2026-05-13T00:00:00.000Z'),
      to: new Date('2026-06-12T00:00:00.000Z')
    })).resolves.toMatchObject({
      items: [{ api_request_count: 25 }],
      query: { cache_hit: true, job_bytes_billed: 12, job_bytes_processed: 34 }
    });
  });

  it('does not store non-default vault metric ranges', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });
    const input = {
      from: new Date('2026-06-10T00:00:00.000Z'),
      grain: 'hour' as const,
      to: new Date('2026-06-10T03:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    };

    await cache.setVaultMetrics(input, {
      items: [{ api_error_count: 0, api_rate_limited_count: 0, api_request_count: 10, bucket_ts: '2026-06-10T00:00:00.000Z' }] as never,
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    expect(store.docs.size).toBe(0);
    await expect(cache.getVaultMetrics(input)).resolves.toBeNull();
  });

  it('does not store oversized default vault UI snapshots', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });

    await cache.setVaultMetrics({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      vaultId: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6',
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    }, {
      items: Array.from({ length: 800 }, (_, index) => ({
        api_error_count: index,
        api_rate_limited_count: index,
        api_request_count: index,
        bucket_ts: `2026-06-10T${String(index % 24).padStart(2, '0')}:00:00.000Z`,
        completion_tokens: index,
        duration_p95_ms: null,
        embedding_input_chars: index,
        embedding_input_tokens: index,
        ingest_events_delta: index,
        memory_adds_delta: index,
        memory_count_delta: index,
        model_request_count: index,
        prompt_tokens: index,
        searches_delta: index,
        storage_bytes_delta: index,
        total_tokens: index
      })),
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    expect(store.docs.size).toBe(0);
  });

  it('stores and reads only default top-vault UI snapshots', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });
    const input = {
      from: new Date('2026-05-12T00:00:00.000Z'),
      limit: 5,
      metric: 'api_requests' as const,
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    };

    await cache.setTopVaults(input, {
      items: [{
        api_error_count: 0,
        api_rate_limited_count: 0,
        api_request_count: 25,
        ingest_events_delta: 0,
        memory_adds_delta: 0,
        memory_count_delta: 0,
        metric_value: 25,
        searches_delta: 0,
        vault_id: '57a89ef6-5fee-4106-956d-1ac3cfa85dd6'
      }],
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    expect(store.docs.size).toBe(1);
    const result = await cache.getTopVaults(input);
    expect(result).toMatchObject({
      items: [{ metric_value: 25 }],
      query: { cache_hit: true, job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    await cache.setTopVaults({
      ...input,
      limit: 10,
      metric: 'searches'
    }, {
      items: [],
      query: { job_bytes_billed: 1, job_bytes_processed: 1 }
    });

    expect(store.docs.size).toBe(1);
  });

  it('stores and reads only default daily workspace snapshots', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });

    await cache.setWorkspaceSummary({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    }, {
      items: [{ api_error_count: 0, api_rate_limited_count: 0, api_request_count: 10, bucket_ts: '2026-06-10T00:00:00.000Z' }] as never,
      query: { job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    expect(store.docs.size).toBe(1);
    const result = await cache.getWorkspaceSummary({
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    });

    expect(result).toMatchObject({
      items: [{ api_request_count: 10 }],
      query: { cache_hit: true, job_bytes_billed: 42, job_bytes_processed: 128 }
    });

    await cache.setWorkspaceSummary({
      from: new Date('2026-06-10T00:00:00.000Z'),
      grain: 'day',
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    }, {
      items: [],
      query: { job_bytes_billed: 1, job_bytes_processed: 1 }
    });

    expect(store.docs.size).toBe(1);
  });

  it('does not return expired snapshots', async () => {
    const store = firestoreStore();
    const cache = new FirestoreAnalyticsSnapshotCache({
      collectionId: 'customer_metric_snapshots',
      firestore: store.firestore,
      ttlSeconds: 3600
    });
    const input = {
      from: new Date('2026-05-12T00:00:00.000Z'),
      grain: 'day' as const,
      to: new Date('2026-06-11T00:00:00.000Z'),
      workspaceId: '1f3b47e1-c0e9-4b76-bb93-88ef62d788ee'
    };

    await cache.setWorkspaceSummary(input, {
      items: [],
      query: { job_bytes_billed: 1, job_bytes_processed: 1 }
    });
    const doc = Array.from(store.docs.values())[0] as Record<string, unknown>;
    doc.expires_at = new Date('2026-01-01T00:00:00.000Z');

    const result = await cache.getWorkspaceSummary(input);

    expect(result).toBeNull();
  });
});
