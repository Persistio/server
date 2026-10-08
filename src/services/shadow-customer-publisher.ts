import { v1 } from '@google-cloud/pubsub';
import type { CustomerMetricEvent, CustomerMetricPublisher } from './customer-metrics';

/** No batching timer or SDK retry; SQL owns identity, retry and destination. */
class BoundedPubSubPublisher implements CustomerMetricPublisher {
  private readonly client: InstanceType<typeof v1.PublisherClient>;
  constructor(private readonly topic: string) {
    this.client = new v1.PublisherClient();
  }
  async publish(events: CustomerMetricEvent[]): Promise<void> {
    if (events.length !== 1) throw new Error('Customer publisher requires one bounded event');
    await this.client.publish({topic:this.topic,messages:events.map(event => ({
      data:Buffer.from(JSON.stringify({...event,...(event.labels ? {labels:JSON.stringify(event.labels)} : {})})),
      attributes:{contract_version:String(event.contract_version),event_id:event.event_id,event_ts:event.event_ts,
        event_date:event.event_date,event_type:event.event_type,source:event.source,
        vault_id:event.vault_id ?? '',workspace_id:event.workspace_id}
    }))}, {timeout:4000,retry:null});
  }
  async close(): Promise<void> { await this.client.close(); }
}

export class ShadowPubSubPublisher extends BoundedPubSubPublisher {
  constructor(topic: string, productionTopic: string) {
    const match = /^projects\/([a-z][a-z0-9-]{4,61}[a-z0-9])\/topics\/([A-Za-z][A-Za-z0-9._~+%-]{2,254}-shadow)$/.exec(topic);
    if (!match || !productionTopic || topic === productionTopic || match[2] === productionTopic.split('/').pop()) {
      throw new Error('Use a distinct fully qualified -shadow topic and declare the production topic');
    }
    super(topic);
  }
}

export class ProductionCustomerPublisher extends BoundedPubSubPublisher {
  constructor(topic: string) {
    if (!/^projects\/[a-z][a-z0-9-]{4,61}[a-z0-9]\/topics\/[A-Za-z][A-Za-z0-9._~+%-]{2,254}$/.test(topic) || topic.endsWith('-shadow')) {
      throw new Error('Use a fully qualified production customer topic');
    }
    super(topic);
  }
}
