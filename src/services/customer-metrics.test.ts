import { afterEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../config';
import {
  CUSTOMER_METRICS_CONTRACT_VERSION,
  CustomerMetricEmitter,
  GcpPubSubCustomerMetricPublisher,
  LogCustomerMetricPublisher,
  NoopCustomerMetricPublisher,
  buildCustomerMetricEvent,
  createConfiguredCustomerMetricPublisher,
  flushCustomerMetrics,
  initCustomerMetrics,
  recordCustomerMetric,
  shutdownCustomerMetrics,
  shouldRecordCustomerMetrics,
  type CustomerMetricEvent,
  type CustomerMetricPublisher
} from './customer-metrics';

const workspaceId = 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef';
const vaultId = 'dff718f2-9d97-43b2-a3cc-a14099ed42c3';

function appConfig(overrides: Partial<AppConfig>): AppConfig {
  return {
    CUSTOMER_METRICS_BATCH_SIZE: 100,
    CUSTOMER_METRICS_FLUSH_INTERVAL_MS: 5000,
    CUSTOMER_METRICS_GCP_PUBSUB_PROJECT_ID: '',
    CUSTOMER_METRICS_GCP_PUBSUB_TOPIC: '',
    CUSTOMER_METRICS_PUBLISHER: 'noop',
    ...overrides
  } as AppConfig;
}

function metricEvent(index = 0): CustomerMetricEvent {
  return buildCustomerMetricEvent({
    api_request_count: 1,
    duration_ms: 12 + index,
    event_id: `event-${index}`,
    event_ts: '2026-06-01T12:34:56.789Z',
    event_type: 'api_request',
    method: 'GET',
    operation: 'recall',
    route: '/v1/recall',
    source: 'api',
    status_code: 200,
    vault_id: vaultId,
    workspace_id: workspaceId
  });
}

describe('customer metric event contract', () => {
  it('builds contract-versioned events with UTC event dates', () => {
    const event = metricEvent();

    expect(event).toMatchObject({
      api_request_count: 1,
      contract_version: CUSTOMER_METRICS_CONTRACT_VERSION,
      event_date: '2026-06-01',
      event_id: 'event-0',
      event_ts: '2026-06-01T12:34:56.789Z',
      event_type: 'api_request',
      workspace_id: workspaceId
    });
  });

  it('keeps memory capacity deltas distinct from memory-add quota deltas', () => {
    const event = buildCustomerMetricEvent({
      event_id: 'event-memory-count',
      event_ts: '2026-06-01T00:00:00.000Z',
      event_type: 'quota_delta',
      memory_count_delta: -1,
      operation: 'memory_count',
      source: 'worker',
      vault_id: vaultId,
      workspace_id: workspaceId
    });

    expect(event).toMatchObject({
      event_type: 'quota_delta',
      memory_count_delta: -1,
      operation: 'memory_count'
    });
    expect('memory_adds_delta' in event).toBe(false);
  });

  it('builds each supported quota delta dimension', () => {
    expect(buildCustomerMetricEvent({
      event_type: 'quota_delta',
      ingest_events_delta: 1,
      operation: 'ingest_events',
      source: 'api',
      vault_id: vaultId,
      workspace_id: workspaceId
    })).toMatchObject({ ingest_events_delta: 1, operation: 'ingest_events' });

    expect(buildCustomerMetricEvent({
      event_type: 'quota_delta',
      memory_adds_delta: 1,
      operation: 'memory_adds',
      source: 'api',
      vault_id: vaultId,
      workspace_id: workspaceId
    })).toMatchObject({ memory_adds_delta: 1, operation: 'memory_adds' });

    expect(buildCustomerMetricEvent({
      event_type: 'quota_delta',
      searches_delta: 1,
      operation: 'searches',
      source: 'api',
      vault_id: vaultId,
      workspace_id: workspaceId
    })).toMatchObject({ operation: 'searches', searches_delta: 1 });
  });

  it('builds model usage and storage delta events', () => {
    expect(buildCustomerMetricEvent({
      event_id: 'event-model-usage',
      event_ts: '2026-06-01T00:00:00.000Z',
      event_type: 'model_usage',
      model: 'gemini-2.5-flash',
      model_request_count: 1,
      model_role: 'extraction',
      prompt_tokens: 100,
      provider: 'google',
      source: 'extraction_worker',
      total_tokens: 125,
      vault_id: vaultId,
      workspace_id: workspaceId
    })).toMatchObject({
      event_type: 'model_usage',
      model_request_count: 1,
      model_role: 'extraction',
      total_tokens: 125
    });

    expect(buildCustomerMetricEvent({
      event_id: 'event-storage-delta',
      event_ts: '2026-06-01T00:00:00.000Z',
      event_type: 'storage_delta',
      operation: 'raw_chunk_blob_write',
      source: 'api',
      storage_bytes_delta: 2048,
      vault_id: vaultId,
      workspace_id: workspaceId
    })).toMatchObject({
      event_type: 'storage_delta',
      operation: 'raw_chunk_blob_write',
      storage_bytes_delta: 2048
    });
  });
});

describe('customer metric publishers', () => {
  it('does nothing in noop mode', async () => {
    await expect(new NoopCustomerMetricPublisher().publish([metricEvent()])).resolves.toBeUndefined();
  });

  it('logs batch shape in log mode without logging full events', async () => {
    const logger = { info: vi.fn() };
    const publisher = new LogCustomerMetricPublisher(logger);

    await publisher.publish([metricEvent(1), metricEvent(2)]);

    expect(logger.info).toHaveBeenCalledWith({
      count: 2,
      event_types: ['api_request']
    }, 'customer metric events accepted');
  });

  it('publishes each metric as an individual Pub/Sub message with stable attributes', async () => {
    const publishMessage = vi.fn().mockResolvedValue('message-1');
    const closeClient = vi.fn().mockResolvedValue(undefined);
    const client = {
      close: closeClient,
      topic: vi.fn().mockReturnValue({ publishMessage })
    };
    const publisher = new GcpPubSubCustomerMetricPublisher(client, 'customer-metrics');
    const event: CustomerMetricEvent = {
      ...metricEvent(),
      labels: {
        auth_method: 'oauth'
      }
    };

    await publisher.publish([event]);
    await publisher.close();

    expect(client.topic).toHaveBeenCalledWith('customer-metrics');
    expect(publishMessage).toHaveBeenCalledWith({
      attributes: {
        contract_version: '1',
        event_date: event.event_date,
        event_id: event.event_id,
        event_type: event.event_type,
        source: event.source,
        vault_id: vaultId,
        workspace_id: workspaceId
      },
      json: {
        ...event,
        labels: JSON.stringify(event.labels)
      }
    });
    expect(closeClient).toHaveBeenCalledOnce();
  });

  it('selects publishers from config', async () => {
    expect(await createConfiguredCustomerMetricPublisher(appConfig({
      CUSTOMER_METRICS_PUBLISHER: 'noop'
    }))).toBeInstanceOf(NoopCustomerMetricPublisher);

    expect(await createConfiguredCustomerMetricPublisher(appConfig({
      CUSTOMER_METRICS_PUBLISHER: 'log'
    }))).toBeInstanceOf(LogCustomerMetricPublisher);

    expect(shouldRecordCustomerMetrics(appConfig({ CUSTOMER_METRICS_PUBLISHER: 'gcp_pubsub' }))).toBe(true);
  });

  it('reports whether customer metrics should be recorded', () => {
    expect(shouldRecordCustomerMetrics(appConfig({ CUSTOMER_METRICS_PUBLISHER: 'noop' }))).toBe(false);
    expect(shouldRecordCustomerMetrics(appConfig({ CUSTOMER_METRICS_PUBLISHER: 'log' }))).toBe(true);
    expect(shouldRecordCustomerMetrics(appConfig({ CUSTOMER_METRICS_PUBLISHER: 'gcp_pubsub' }))).toBe(true);
  });
});

describe('customer metric emitter', () => {
  it('publishes immediately when the batch threshold is reached', async () => {
    const publisher = new RecordingPublisher();
    const emitter = new CustomerMetricEmitter(publisher, {
      batchSize: 2,
      flushIntervalMs: 5000
    });

    emitter.record(metricEvent(1));
    emitter.record(metricEvent(2));
    await emitter.flush();

    expect(publisher.batches.map((batch) => batch.map((event) => event.event_id))).toEqual([
      ['event-1', 'event-2']
    ]);
  });

  it('drains every queued batch before close', async () => {
    const publisher = new RecordingPublisher();
    const emitter = new CustomerMetricEmitter(publisher, {
      batchSize: 2,
      flushIntervalMs: 5000
    });

    emitter.record(metricEvent(1));
    emitter.record(metricEvent(2));
    emitter.record(metricEvent(3));
    emitter.record(metricEvent(4));
    emitter.record(metricEvent(5));
    await emitter.close();

    expect(publisher.closed).toBe(true);
    expect(publisher.batches.map((batch) => batch.map((event) => event.event_id))).toEqual([
      ['event-1', 'event-2'],
      ['event-3', 'event-4'],
      ['event-5']
    ]);
  });

  it('logs publish failures without throwing into callers', async () => {
    const publisher = new RecordingPublisher(new Error('publish failed'));
    const logger = { warn: vi.fn() };
    const emitter = new CustomerMetricEmitter(publisher, {
      batchSize: 2,
      flushIntervalMs: 5000,
      logger
    });

    emitter.record(metricEvent(1));
    await expect(emitter.flush()).resolves.toBeUndefined();

    expect(logger.warn).toHaveBeenCalledWith({
      count: 1,
      error: 'publish failed'
    }, 'failed to publish customer metric events');
  });
});

class RecordingPublisher implements CustomerMetricPublisher {
  batches: CustomerMetricEvent[][] = [];
  closed = false;

  constructor(private readonly publishError?: Error) {}

  async publish(events: CustomerMetricEvent[]): Promise<void> {
    if (this.publishError) throw this.publishError;
    this.batches.push(events);
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

describe('customer metric lifecycle', () => {
  afterEach(async () => {
    await shutdownCustomerMetrics();
  });

  it('records through the configured singleton emitter', async () => {
    const logger = { info: vi.fn() };
    await initCustomerMetrics(appConfig({
      CUSTOMER_METRICS_BATCH_SIZE: 10,
      CUSTOMER_METRICS_PUBLISHER: 'log'
    }), logger);

    recordCustomerMetric({
      api_request_count: 1,
      duration_ms: 15,
      event_type: 'api_request',
      method: 'GET',
      operation: 'recall',
      route: '/v1/recall',
      source: 'api',
      status_code: 200,
      vault_id: vaultId,
      workspace_id: workspaceId
    });
    await flushCustomerMetrics();

    expect(logger.info).toHaveBeenCalledWith({
      customer_metrics_publisher: 'log'
    }, 'Customer metrics configured');
    expect(logger.info).toHaveBeenCalledWith({
      count: 1,
      event_types: ['api_request']
    }, 'customer metric events accepted');
  });

  it('lets record calls before initialization no-op', async () => {
    recordCustomerMetric(metricEvent());

    await expect(flushCustomerMetrics()).resolves.toBeUndefined();
  });

  it('closes the previous emitter when initialized again', async () => {
    const firstLogger = { info: vi.fn() };
    const secondLogger = { info: vi.fn() };

    await initCustomerMetrics(appConfig({
      CUSTOMER_METRICS_BATCH_SIZE: 10,
      CUSTOMER_METRICS_PUBLISHER: 'log'
    }), firstLogger);
    recordCustomerMetric(metricEvent(1));

    await initCustomerMetrics(appConfig({
      CUSTOMER_METRICS_BATCH_SIZE: 10,
      CUSTOMER_METRICS_PUBLISHER: 'log'
    }), secondLogger);

    expect(firstLogger.info).toHaveBeenCalledWith({
      count: 1,
      event_types: ['api_request']
    }, 'customer metric events accepted');
  });

  it('drains queued metrics during shutdown', async () => {
    const logger = { info: vi.fn() };
    await initCustomerMetrics(appConfig({
      CUSTOMER_METRICS_BATCH_SIZE: 10,
      CUSTOMER_METRICS_PUBLISHER: 'log'
    }), logger);
    recordCustomerMetric(metricEvent());

    await shutdownCustomerMetrics();

    expect(logger.info).toHaveBeenCalledWith({
      count: 1,
      event_types: ['api_request']
    }, 'customer metric events accepted');
  });
});
