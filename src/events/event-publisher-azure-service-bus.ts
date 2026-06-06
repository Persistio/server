import { DefaultAzureCredential } from '@azure/identity';
import { ServiceBusClient, type ServiceBusSender } from '@azure/service-bus';

import type { AppConfig } from '../config';
import type { EventPublisher } from './event-publisher';
import type { PlatformEvent } from './platform-event';

export class AzureServiceBusEventPublisher implements EventPublisher {
  private readonly sender: ServiceBusSender;

  constructor(
    private readonly client: ServiceBusClient,
    topicOrQueueName: string
  ) {
    this.sender = client.createSender(topicOrQueueName);
  }

  async close(): Promise<void> {
    try {
      await this.sender.close();
    } finally {
      await this.client.close();
    }
  }

  async publish(event: PlatformEvent<string>): Promise<void> {
    await this.sender.sendMessages({
      body: event,
      contentType: 'application/json',
      messageId: event.event_id,
      subject: event.event_type,
      applicationProperties: {
        event_type: event.event_type,
        schema_version: event.schema_version,
        subject: event.subject
      }
    });
  }
}

export function createAzureServiceBusEventPublisher(config: AppConfig): EventPublisher {
  const client = config.AZURE_SERVICE_BUS_CONNECTION_STRING
    ? new ServiceBusClient(config.AZURE_SERVICE_BUS_CONNECTION_STRING)
    : new ServiceBusClient(
      config.AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE,
      new DefaultAzureCredential()
    );

  return new AzureServiceBusEventPublisher(client, config.AZURE_SERVICE_BUS_TOPIC_OR_QUEUE);
}
