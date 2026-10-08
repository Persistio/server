import { beforeEach, describe, expect, it, vi } from 'vitest';
const rpc=vi.hoisted(()=>({publish:vi.fn(),close:vi.fn(),constructed:vi.fn()}));
vi.mock('@google-cloud/pubsub',()=>({v1:{PublisherClient:class {constructor(){rpc.constructed();}publish=rpc.publish;close=rpc.close;}}}));
import { ShadowPubSubPublisher, ProductionCustomerPublisher } from './shadow-customer-publisher';
import { buildCustomerMetricEvent } from './customer-metrics';
beforeEach(()=>{vi.resetAllMocks();});
describe('isolated bounded Pub/Sub shadow adapter',()=>{
  it.each(['events','projects/example-project/topics/events','projects/example-project/topics/customer-shadow'])(
    'rejects incomplete or production destinations: %s',topic=>{
      expect(()=>new ShadowPubSubPublisher(topic,'customer-shadow')).toThrow();
      expect(rpc.constructed).not.toHaveBeenCalled();
    });
  it('sends the exact v1 fields/attributes, disables SDK retries and batching, and closes resources',async()=>{
    const publisher=new ShadowPubSubPublisher('projects/example-project/topics/customer-shadow','customer');
    const event=buildCustomerMetricEvent({event_type:'api_request',api_request_count:1,operation:'recall',route:'/v1/recall',
      duration_ms:3,method:'POST',status_code:200,source:'api',workspace_id:'workspace',labels:{auth_method:'oauth'}});
    await publisher.publish([event]);
    const [request,options]=rpc.publish.mock.calls[0];
    expect(options).toEqual({timeout:4000,retry:null});
    expect(request.topic).toBe('projects/example-project/topics/customer-shadow');
    expect(JSON.parse(request.messages[0].data.toString())).toEqual({...event,labels:JSON.stringify(event.labels)});
    expect(request.messages[0].attributes).toMatchObject({event_id:event.event_id,event_date:event.event_date,workspace_id:'workspace'});
    await publisher.close();expect(rpc.close).toHaveBeenCalledOnce();
    await expect(publisher.publish([event,event])).rejects.toThrow('one bounded');
  });
});

it('production adapter rejects shadow/short topics before client discovery and retains the event envelope', async () => {
  for (const topic of ['customer', 'projects/example-project/topics/customer-shadow']) {
    expect(() => new ProductionCustomerPublisher(topic)).toThrow();
  }
  expect(rpc.constructed).not.toHaveBeenCalled();
  const publisher = new ProductionCustomerPublisher('projects/example-project/topics/customer');
  const event = buildCustomerMetricEvent({event_type:'quota_delta',operation:'memory_count',memory_count_delta:1,
    source:'worker',workspace_id:'workspace',vault_id:'vault'});
  await publisher.publish([event]);
  expect(rpc.publish.mock.calls[0][0].topic).toBe('projects/example-project/topics/customer');
  expect(JSON.parse(rpc.publish.mock.calls[0][0].messages[0].data.toString())).toEqual(event);
  await publisher.close();
});
