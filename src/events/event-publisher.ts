import crypto from 'node:crypto';

import type { AppConfig } from '../config';
import type { PlatformEvent } from './platform-event';

const DEFAULT_WEBHOOK_TIMEOUT_MS = 30000;

export interface EventPublisher {
  close?(): Promise<void>;
  publish(event: PlatformEvent<string>): Promise<void>;
}

export interface EventPublisherLogger {
  warn?(details: unknown, message?: string): void;
  info?(details: unknown, message?: string): void;
}

export class NoopEventPublisher implements EventPublisher {
  constructor(private readonly logger?: EventPublisherLogger) {}

  async publish(event: PlatformEvent<string>): Promise<void> {
    this.logger?.info?.({
      event_id: event.id,
      event_type: event.type,
      subject: event.subject
    }, 'No-op platform event publisher accepted event');
  }
}

interface WebhookEventPublisherOptions {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

export class WebhookEventPublisher implements EventPublisher {
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;

  constructor(
    private readonly url: string,
    private readonly secret: string,
    optionsOrFetchImpl: WebhookEventPublisherOptions | typeof fetch = {}
  ) {
    const options = typeof optionsOrFetchImpl === 'function'
      ? { fetchImpl: optionsOrFetchImpl }
      : optionsOrFetchImpl;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_WEBHOOK_TIMEOUT_MS;
  }

  async publish(event: PlatformEvent<string>): Promise<void> {
    const body = JSON.stringify(event);
    const signature = crypto
      .createHmac('sha256', this.secret)
      .update(body)
      .digest('hex');
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
    }, this.timeoutMs);
    let response: Response;

    try {
      response = await this.fetchImpl(this.url, {
        body,
        headers: {
          'Content-Type': 'application/json',
          'X-Persistio-Event-Id': event.id,
          'X-Persistio-Event-Type': event.type,
          'X-Persistio-Signature': signature
        },
        method: 'POST',
        redirect: 'manual',
        signal: controller.signal
      });
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error(`Event webhook publish timed out after ${this.timeoutMs}ms`);
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }

    if (response.status >= 300 && response.status < 400) {
      throw new Error(`Event webhook publish redirected with status ${response.status}`);
    }

    if (!response.ok) {
      throw new Error(`Event webhook publish failed with status ${response.status}`);
    }
  }
}

export async function createConfiguredEventPublisher(
  config: AppConfig,
  logger?: EventPublisherLogger
): Promise<EventPublisher> {
  switch (config.EVENT_PUBLISHER) {
    case 'noop':
      if (config.NODE_ENV === 'production') {
        logger?.warn?.({}, 'EVENT_PUBLISHER=noop is active in production; platform events will not leave this process');
      }
      return new NoopEventPublisher(logger);
    case 'webhook':
      return new WebhookEventPublisher(config.PLATFORM_EVENTS_WEBHOOK_URL, config.PLATFORM_EVENTS_WEBHOOK_SECRET, {
        timeoutMs: config.PLATFORM_EVENTS_WEBHOOK_TIMEOUT_MS
      });
    case 'azure_service_bus': {
      const { createAzureServiceBusEventPublisher } = await import('./event-publisher-azure-service-bus');
      return createAzureServiceBusEventPublisher(config);
    }
    case 'gcp_pubsub': {
      const { createGcpPubSubEventPublisher } = await import('./event-publisher-gcp-pubsub');
      return createGcpPubSubEventPublisher(config);
    }
  }
}

export function shouldDispatchPlatformEvents(
  config: Pick<AppConfig, 'EVENT_PUBLISHER' | 'PERSISTIO_MODE'>
): boolean {
  return config.PERSISTIO_MODE !== 'api' && config.EVENT_PUBLISHER !== 'noop';
}
