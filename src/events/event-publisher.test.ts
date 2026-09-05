import crypto from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../config';
import { AzureServiceBusEventPublisher } from './event-publisher-azure-service-bus';
import { GcpPubSubEventPublisher } from './event-publisher-gcp-pubsub';
import {
  NoopEventPublisher,
  WebhookEventPublisher,
  createConfiguredEventPublisher,
  shouldDispatchPlatformEvents
} from './event-publisher';
import { buildPlatformEvent, usagePeriodClosedEventType, type PlatformEvent } from './platform-event';

const event: PlatformEvent<string> = buildPlatformEvent({
  category: 'usage',
  data: {
    account_id: 'workspace-1',
    period: '2026-05',
    platform_vault_id: 'vault-1',
    plan_id: 'unlimited',
    usage: { ingest_events: 1 },
    limits: { ingest_events_per_month: 10 },
    sensitivity: 'metadata_only',
    summary: 'Usage period 2026-05 closed',
    vault_id: 'vault-1',
    workspace_id: 'workspace-1'
  },
  id: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
  occurredAt: '2026-06-01T00:00:03.000Z',
  subject: 'vault/vault-1',
  type: usagePeriodClosedEventType,
  vaultId: 'vault-1',
  workspaceId: 'workspace-1'
});

function appConfig(overrides: Partial<AppConfig>): AppConfig {
  return {
    EVENT_PUBLISHER: 'noop',
    NODE_ENV: 'test',
    PLATFORM_EVENTS_WEBHOOK_SECRET: '',
    PLATFORM_EVENTS_WEBHOOK_TIMEOUT_MS: 30000,
    PLATFORM_EVENTS_WEBHOOK_URL: '',
    AZURE_SERVICE_BUS_CONNECTION_STRING: '',
    AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE: '',
    AZURE_SERVICE_BUS_TOPIC_OR_QUEUE: '',
    GCP_PUBSUB_PROJECT_ID: '',
    GCP_PUBSUB_TOPIC: '',
    ...overrides
  } as AppConfig;
}

describe('event publishers', () => {
  it('logs and returns in noop mode', async () => {
    const logger = { info: vi.fn() };
    const publisher = new NoopEventPublisher(logger);

    await expect(publisher.publish(event)).resolves.toBeUndefined();

    expect(logger.info).toHaveBeenCalledWith(
      {
        event_id: event.id,
        event_type: event.type,
        subject: event.subject
      },
      'No-op platform event publisher accepted event'
    );
  });

  it('signs and posts webhook events', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 202 });
    const publisher = new WebhookEventPublisher(
      'https://app.example/events',
      'secret',
      fetchMock as unknown as typeof fetch
    );
    const body = JSON.stringify(event);

    await publisher.publish(event);

    expect(fetchMock).toHaveBeenCalledWith('https://app.example/events', {
      body,
      headers: {
        'Content-Type': 'application/json',
        'X-Persistio-Event-Id': event.id,
        'X-Persistio-Event-Type': event.type,
        'X-Persistio-Signature': crypto.createHmac('sha256', 'secret').update(body).digest('hex')
      },
      method: 'POST',
      redirect: 'manual',
      signal: expect.any(AbortSignal)
    });
  });

  it('fails webhook publishing on non-2xx responses', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    const publisher = new WebhookEventPublisher(
      'https://app.example/events',
      'secret',
      fetchMock as unknown as typeof fetch
    );

    await expect(publisher.publish(event)).rejects.toThrow('Event webhook publish failed with status 503');
  });

  it('fails webhook publishing on redirects', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 302 });
    const publisher = new WebhookEventPublisher(
      'https://app.example/events',
      'secret',
      fetchMock as unknown as typeof fetch
    );

    await expect(publisher.publish(event)).rejects.toThrow('Event webhook publish redirected with status 302');
  });

  it('aborts webhook publishing after the configured timeout', async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn((_url, init) => new Promise((_resolve, reject) => {
      const signal = (init as RequestInit).signal as AbortSignal;
      signal.addEventListener('abort', () => reject(new Error('aborted')));
    }));
    const publisher = new WebhookEventPublisher(
      'https://app.example/events',
      'secret',
      {
        fetchImpl: fetchMock as unknown as typeof fetch,
        timeoutMs: 100
      }
    );

    try {
      const publish = expect(publisher.publish(event)).rejects.toThrow('Event webhook publish timed out after 100ms');
      await vi.advanceTimersByTimeAsync(100);

      await publish;
    } finally {
      vi.useRealTimers();
    }
  });

  it('publishes Azure Service Bus messages with event metadata', async () => {
    const sendMessages = vi.fn().mockResolvedValue(undefined);
    const closeSender = vi.fn().mockResolvedValue(undefined);
    const closeClient = vi.fn().mockResolvedValue(undefined);
    const client = {
      close: closeClient,
      createSender: vi.fn().mockReturnValue({ close: closeSender, sendMessages })
    };
    const publisher = new AzureServiceBusEventPublisher(client as never, 'platform-events');

    await publisher.publish(event);
    await publisher.close();

    expect(client.createSender).toHaveBeenCalledWith('platform-events');
    expect(sendMessages).toHaveBeenCalledWith({
      body: event,
      contentType: 'application/json',
      messageId: event.id,
      subject: event.type,
      applicationProperties: {
        event_type: event.type,
        specversion: event.specversion,
        subject: event.subject
      }
    });
    expect(closeSender).toHaveBeenCalledOnce();
    expect(closeClient).toHaveBeenCalledOnce();
  });

  it('closes the Azure Service Bus client when sender cleanup fails', async () => {
    const closeClient = vi.fn().mockResolvedValue(undefined);
    const client = {
      close: closeClient,
      createSender: vi.fn().mockReturnValue({
        close: vi.fn().mockRejectedValue(new Error('sender close failed')),
        sendMessages: vi.fn()
      })
    };
    const publisher = new AzureServiceBusEventPublisher(client as never, 'platform-events');

    await expect(publisher.close()).rejects.toThrow('sender close failed');

    expect(closeClient).toHaveBeenCalledOnce();
  });

  it('publishes GCP Pub/Sub messages with event metadata', async () => {
    const publishMessage = vi.fn().mockResolvedValue('message-1');
    const closeClient = vi.fn().mockResolvedValue(undefined);
    const client = {
      close: closeClient,
      topic: vi.fn().mockReturnValue({ publishMessage })
    };
    const publisher = new GcpPubSubEventPublisher(client, 'platform-events');

    await publisher.publish(event);
    await publisher.close();

    expect(client.topic).toHaveBeenCalledWith('platform-events');
    expect(publishMessage).toHaveBeenCalledWith({
      json: {
        ...event,
        data_json: JSON.stringify(event.data)
      },
      attributes: {
        event_id: event.id,
        event_type: event.type,
        specversion: event.specversion,
        subject: event.subject
      }
    });
    expect(closeClient).toHaveBeenCalledOnce();
  });

  it('selects noop and warns in production', async () => {
    const logger = { warn: vi.fn() };
    const publisher = await createConfiguredEventPublisher(appConfig({
      EVENT_PUBLISHER: 'noop',
      NODE_ENV: 'production'
    }), logger);

    expect(publisher).toBeInstanceOf(NoopEventPublisher);
    expect(logger.warn).toHaveBeenCalledWith(
      {},
      'EVENT_PUBLISHER=noop is active in production; platform events will not leave this process'
    );
  });

  it('selects webhook publisher from config', async () => {
    const publisher = await createConfiguredEventPublisher(appConfig({
      EVENT_PUBLISHER: 'webhook',
      PLATFORM_EVENTS_WEBHOOK_SECRET: 'secret',
      PLATFORM_EVENTS_WEBHOOK_URL: 'https://app.example/events'
    }));

    expect(publisher).toBeInstanceOf(WebhookEventPublisher);
  });

  it('does not start outbox dispatching for noop publishers', () => {
    expect(shouldDispatchPlatformEvents(appConfig({
      EVENT_PUBLISHER: 'noop',
      PERSISTIO_MODE: 'combined'
    }))).toBe(false);
    expect(shouldDispatchPlatformEvents(appConfig({
      EVENT_PUBLISHER: 'noop',
      PERSISTIO_MODE: 'worker'
    }))).toBe(false);
  });

  it('starts outbox dispatching only for configured transports in worker-capable modes', () => {
    expect(shouldDispatchPlatformEvents(appConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'worker'
    }))).toBe(true);
    expect(shouldDispatchPlatformEvents(appConfig({
      EVENT_PUBLISHER: 'azure_service_bus',
      PERSISTIO_MODE: 'combined'
    }))).toBe(true);
    expect(shouldDispatchPlatformEvents(appConfig({
      EVENT_PUBLISHER: 'gcp_pubsub',
      PERSISTIO_MODE: 'worker'
    }))).toBe(true);
    expect(shouldDispatchPlatformEvents(appConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'api'
    }))).toBe(false);
  });
});
