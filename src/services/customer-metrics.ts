import { randomUUID } from 'node:crypto';

import type { AppConfig } from '../config';

export const CUSTOMER_METRICS_CONTRACT_VERSION = 1;

export type CustomerMetricEventType = 'api_request' | 'quota_delta' | 'model_usage' | 'storage_delta';
export type CustomerMetricSource = 'api' | 'worker' | 'extraction_worker' | 'curation_worker' | 'system';
export type CustomerMetricLabelValue = string | number | boolean | null;

interface CustomerMetricEnvelope {
  contract_version: 1;
  event_date: string;
  event_id: string;
  event_ts: string;
  event_type: CustomerMetricEventType;
  labels?: Record<string, CustomerMetricLabelValue>;
  source: CustomerMetricSource;
  vault_id?: string;
  workspace_id: string;
}

export interface ApiRequestMetricEvent extends CustomerMetricEnvelope {
  api_request_count: 1;
  duration_ms: number;
  event_type: 'api_request';
  method: string;
  operation: string;
  route: string;
  status_code: number;
}

interface QuotaDeltaMetricBase extends CustomerMetricEnvelope {
  event_type: 'quota_delta';
  vault_id: string;
}

type QuotaDeltaMetricDimension =
  | {
    ingest_events_delta: number;
    memory_adds_delta?: never;
    memory_count_delta?: never;
    operation: 'ingest_events';
    searches_delta?: never;
  }
  | {
    ingest_events_delta?: never;
    memory_adds_delta: number;
    memory_count_delta?: never;
    operation: 'memory_adds';
    searches_delta?: never;
  }
  | {
    ingest_events_delta?: never;
    memory_adds_delta?: never;
    memory_count_delta: number;
    operation: 'memory_count';
    searches_delta?: never;
  }
  | {
    ingest_events_delta?: never;
    memory_adds_delta?: never;
    memory_count_delta?: never;
    operation: 'searches';
    searches_delta: number;
  };

export type QuotaDeltaMetricEvent = QuotaDeltaMetricBase & QuotaDeltaMetricDimension;

export interface ModelUsageMetricEvent extends CustomerMetricEnvelope {
  completion_tokens?: number;
  embedding_input_chars?: number;
  embedding_input_tokens?: number;
  event_type: 'model_usage';
  model: string;
  model_request_count: number;
  model_role: 'embedding' | 'extraction' | 'escalation' | 'curation';
  prompt_tokens?: number;
  provider: string;
  total_tokens?: number;
}

export interface StorageDeltaMetricEvent extends CustomerMetricEnvelope {
  event_type: 'storage_delta';
  operation: string;
  storage_bytes_delta: number;
}

export type CustomerMetricEvent =
  | ApiRequestMetricEvent
  | ModelUsageMetricEvent
  | QuotaDeltaMetricEvent
  | StorageDeltaMetricEvent;

type EventInput<TEvent extends CustomerMetricEvent> = Omit<TEvent, 'contract_version' | 'event_date' | 'event_id' | 'event_ts'> & {
  event_id?: string;
  event_ts?: Date | string;
};

export type CustomerMetricEventInput = CustomerMetricEvent extends infer TEvent
  ? TEvent extends CustomerMetricEvent
    ? EventInput<TEvent>
    : never
  : never;

export interface CustomerMetricPublisher {
  close?(): Promise<void>;
  publish(events: CustomerMetricEvent[]): Promise<void>;
}

export interface CustomerMetricLogger {
  info?(details: unknown, message?: string): void;
  warn?(details: unknown, message?: string): void;
}

interface PubSubClientLike {
  close?(): Promise<void>;
  topic(name: string): {
    publishMessage(message: {
      attributes?: Record<string, string>;
      json: unknown;
    }): Promise<string>;
  };
}

export class NoopCustomerMetricPublisher implements CustomerMetricPublisher {
  async publish(): Promise<void> {}
}

export class LogCustomerMetricPublisher implements CustomerMetricPublisher {
  constructor(private readonly logger?: CustomerMetricLogger) {}

  async publish(events: CustomerMetricEvent[]): Promise<void> {
    this.logger?.info?.({
      count: events.length,
      event_types: Array.from(new Set(events.map((event) => event.event_type)))
    }, 'customer metric events accepted');
  }
}

export class GcpPubSubCustomerMetricPublisher implements CustomerMetricPublisher {
  private readonly topic;

  constructor(
    private readonly client: PubSubClientLike,
    topicName: string
  ) {
    this.topic = client.topic(topicName);
  }

  async publish(events: CustomerMetricEvent[]): Promise<void> {
    await Promise.all(events.map((event) => this.topic.publishMessage({
      attributes: {
        contract_version: String(event.contract_version),
        event_date: event.event_date,
        event_id: event.event_id,
        event_type: event.event_type,
        source: event.source,
        vault_id: event.vault_id ?? '',
        workspace_id: event.workspace_id
      },
      json: toPubSubBigQueryPayload(event)
    })));
  }

  async close(): Promise<void> {
    await this.client.close?.();
  }
}

export class CustomerMetricEmitter {
  private readonly batchSize: number;
  private readonly flushIntervalMs: number;
  private flushPromise: Promise<void> | null = null;
  private flushTimer: NodeJS.Timeout | null = null;
  private queue: CustomerMetricEvent[] = [];

  constructor(
    private readonly publisher: CustomerMetricPublisher,
    options: {
      batchSize: number;
      flushIntervalMs: number;
      logger?: CustomerMetricLogger;
    }
  ) {
    this.batchSize = options.batchSize;
    this.flushIntervalMs = options.flushIntervalMs;
    this.logger = options.logger;
  }

  private readonly logger?: CustomerMetricLogger;

  record(input: CustomerMetricEventInput): void {
    this.queue.push(buildCustomerMetricEvent(input));
    if (this.queue.length >= this.batchSize) {
      void this.flush();
      return;
    }
    this.scheduleFlush();
  }

  async flush(): Promise<void> {
    if (this.flushPromise) return this.flushPromise;
    this.flushPromise = this.drainQueue();
    try {
      await this.flushPromise;
    } finally {
      this.flushPromise = null;
    }
  }

  async close(): Promise<void> {
    await this.flush();
    await this.publisher.close?.();
  }

  private async drainQueue(): Promise<void> {
    this.clearFlushTimer();
    while (this.queue.length) {
      const batch = this.queue.splice(0, this.batchSize);
      try {
        await this.publisher.publish(batch);
      } catch (error) {
        this.logger?.warn?.({
          count: batch.length,
          error: error instanceof Error ? error.message : String(error)
        }, 'failed to publish customer metric events');
      }
    }
  }

  private clearFlushTimer(): void {
    if (!this.flushTimer) return;
    clearTimeout(this.flushTimer);
    this.flushTimer = null;
  }

  private scheduleFlush(): void {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      void this.flush();
    }, this.flushIntervalMs);
    this.flushTimer.unref?.();
  }
}

export function buildCustomerMetricEvent(input: CustomerMetricEventInput): CustomerMetricEvent {
  const eventTs = normalizeEventTimestamp(input.event_ts);
  const event = {
    ...input,
    contract_version: CUSTOMER_METRICS_CONTRACT_VERSION,
    event_date: eventTs.toISOString().slice(0, 10),
    event_id: input.event_id ?? randomUUID(),
    event_ts: eventTs.toISOString()
  } as CustomerMetricEvent;
  return compactEvent(event);
}

export async function createConfiguredCustomerMetricPublisher(
  config: AppConfig,
  logger?: CustomerMetricLogger
): Promise<CustomerMetricPublisher> {
  switch (config.CUSTOMER_METRICS_PUBLISHER) {
    case 'noop':
      return new NoopCustomerMetricPublisher();
    case 'log':
      return new LogCustomerMetricPublisher(logger);
    case 'gcp_pubsub': {
      const { PubSub } = await import('@google-cloud/pubsub');
      const client = new PubSub({
        projectId: config.CUSTOMER_METRICS_GCP_PUBSUB_PROJECT_ID || undefined
      });
      return new GcpPubSubCustomerMetricPublisher(
        client as unknown as PubSubClientLike,
        config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC
      );
    }
  }
}

export async function createConfiguredCustomerMetricEmitter(
  config: AppConfig,
  logger?: CustomerMetricLogger
): Promise<CustomerMetricEmitter> {
  return new CustomerMetricEmitter(
    await createConfiguredCustomerMetricPublisher(config, logger),
    {
      batchSize: config.CUSTOMER_METRICS_BATCH_SIZE,
      flushIntervalMs: config.CUSTOMER_METRICS_FLUSH_INTERVAL_MS,
      logger
    }
  );
}

export function shouldRecordCustomerMetrics(config: Pick<AppConfig, 'CUSTOMER_METRICS_PUBLISHER'>): boolean {
  return config.CUSTOMER_METRICS_PUBLISHER !== 'noop';
}

let activeEmitter: CustomerMetricEmitter | null = null;

export async function initCustomerMetrics(config: AppConfig, logger?: CustomerMetricLogger): Promise<void> {
  await shutdownCustomerMetrics();
  activeEmitter = await createConfiguredCustomerMetricEmitter(config, logger);
  if (shouldRecordCustomerMetrics(config)) {
    logger?.info?.({
      customer_metrics_publisher: config.CUSTOMER_METRICS_PUBLISHER
    }, 'Customer metrics configured');
  }
}

export function recordCustomerMetric(input: CustomerMetricEventInput): void {
  activeEmitter?.record(input);
}

export async function flushCustomerMetrics(): Promise<void> {
  await activeEmitter?.flush();
}

export async function shutdownCustomerMetrics(): Promise<void> {
  const emitter = activeEmitter;
  activeEmitter = null;
  await emitter?.close();
}

function normalizeEventTimestamp(value: Date | string | undefined): Date {
  const timestamp = value instanceof Date
    ? value
    : value
      ? new Date(value)
      : new Date();
  return Number.isNaN(timestamp.getTime()) ? new Date() : timestamp;
}

function compactEvent<TEvent extends CustomerMetricEvent>(event: TEvent): TEvent {
  return Object.fromEntries(
    Object.entries(event).filter(([, value]) => value !== undefined)
  ) as TEvent;
}

function toPubSubBigQueryPayload(event: CustomerMetricEvent): Record<string, unknown> {
  return {
    ...event,
    ...(event.labels ? { labels: JSON.stringify(event.labels) } : {})
  };
}
