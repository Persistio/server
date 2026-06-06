import { PubSub } from '@google-cloud/pubsub';

import type { AppConfig } from '../config';
import type { EventPublisher } from './event-publisher';
import type { PlatformEvent } from './platform-event';

interface PubSubClientLike {
  close?(): Promise<void>;
  topic(name: string): TopicLike;
}

interface TopicLike {
  publishMessage(message: {
    attributes?: Record<string, string>;
    json: unknown;
  }): Promise<string>;
}

export class GcpPubSubEventPublisher implements EventPublisher {
  private readonly topic: TopicLike;

  constructor(
    private readonly client: PubSubClientLike,
    topicName: string
  ) {
    this.topic = client.topic(topicName);
  }

  async close(): Promise<void> {
    await this.client.close?.();
  }

  async publish(event: PlatformEvent<string>): Promise<void> {
    await this.topic.publishMessage({
      json: event,
      attributes: {
        event_id: event.event_id,
        event_type: event.event_type,
        schema_version: String(event.schema_version),
        subject: event.subject
      }
    });
  }
}

export function createGcpPubSubEventPublisher(config: AppConfig): EventPublisher {
  const client = new PubSub({
    projectId: config.GCP_PUBSUB_PROJECT_ID || undefined
  });

  return new GcpPubSubEventPublisher(client as unknown as PubSubClientLike, config.GCP_PUBSUB_TOPIC);
}
