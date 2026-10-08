import { afterEach, describe, expect, it, vi } from 'vitest';
import { runShadowObservabilityPass } from './shadow-observability-pass';
import { operationalMetadata } from '../operational-metadata';
import type { ShadowDeliveryReport } from './customer-metric-shadow';

const empty: ShadowDeliveryReport = {delivered:0,retried:0,stale:0,dead:0,purged:0};
afterEach(()=>vi.restoreAllMocks());
describe('one-shot shadow delivery status',()=>{
  it.each([
    ['empty',empty,0], ['delivered',{...empty,delivered:3},0], ['retention only',{...empty,purged:4},0],
    ['partial retry',{...empty,delivered:2,retried:1},1], ['stale acknowledgement',{...empty,stale:1},1],
    ['new dead letters',{...empty,dead:1},1], ['mixed failures',{...empty,delivered:1,retried:2,stale:3,dead:4,purged:5},1]
  ] as const)('%s emits exact bounded counts and correct exit status',async(_name,report,status)=>{
    const emit=vi.fn(),observe=vi.fn(async()=>[]);
    expect(await runShadowObservabilityPass({dispatch:async()=>report,observe,emit})).toBe(status);
    expect(observe).toHaveBeenCalledOnce();expect(emit).toHaveBeenCalledOnce();
    expect(emit.mock.calls[0][0]).toEqual({event:'shadow_customer_delivery',shadow_kind:'customer_metrics',
      shadow_outcome:status?'failed':'completed',shadow_health:'ok',duration_ms:expect.any(Number),
      delivery_delivered:report.delivered,delivery_retried:report.retried,delivery_stale:report.stale,
      delivery_dead:report.dead,delivery_purged:report.purged});
    expect(operationalMetadata(emit.mock.calls[0][0])).toEqual(emit.mock.calls[0][0]);
  });
  it('reports thrown delivery as unknown counts, keeps SQL observations and excludes raw errors',async()=>{
    const log=vi.spyOn(console,'info').mockImplementation(()=>{}),observe=vi.fn(async()=>[]);
    expect(await runShadowObservabilityPass({dispatch:async()=>{throw new Error('PRIVATE topic tenant payload');},observe})).toBe(1);
    expect(observe).toHaveBeenCalledOnce();expect(log).toHaveBeenCalledOnce();
    const record=JSON.parse(log.mock.calls[0][0]);
    expect(record).toMatchObject({event:'shadow_customer_delivery',shadow_outcome:'failed',shadow_health:'monitor_error'});
    expect(Object.keys(record).some(k=>k.startsWith('delivery_'))).toBe(false);
    expect(JSON.stringify(record)).not.toContain('PRIVATE');
  });
  it('keeps SQL failure status even after successful delivery',async()=>{
    expect(await runShadowObservabilityPass({dispatch:async()=>empty,emit:()=>{},observe:async()=>[
      {event:'shadow_worker_observation',shadow_kind:'extraction',shadow_health:'monitor_error',observed_at_ms:0}
    ]})).toBe(1);
  });
  it('does not report delivery in observation-only mode and fails on observer exceptions',async()=>{
    const emit=vi.fn();expect(await runShadowObservabilityPass({emit,observe:async()=>[]})).toBe(0);
    expect(await runShadowObservabilityPass({emit,observe:async()=>{throw new Error('PRIVATE SQL');}})).toBe(1);
    expect(emit).not.toHaveBeenCalled();
  });
  it('still observes if the report sink throws',async()=>{
    const observe=vi.fn(async()=>[]);
    expect(await runShadowObservabilityPass({dispatch:async()=>empty,observe,emit:()=>{throw new Error('sink down');}})).toBe(1);
    expect(observe).toHaveBeenCalledOnce();
  });
});
