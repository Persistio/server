import { it, expect, vi } from 'vitest';
const instruments=vi.hoisted(()=>({ add:vi.fn(),record:vi.fn(),callback:vi.fn(),gauge:vi.fn() }));
vi.mock('../metrics',()=>Object.fromEntries(['memoryPolicyEventCounter','recallDeliveryCounter','recallDeliveryMissingAckCounter','globalRuleDeliveryCounter','recallDurationHistogram'].map(name=>[name,instruments])));
vi.mock('../telemetry',()=>({meter:{createObservableGauge:(...args:unknown[])=>{instruments.gauge(...args);return{addCallback:instruments.callback};}}}));
import {createDeliveryHealthCollector,registerDeliveryHealthMetrics} from './delivery-health';
import * as effects from './observability-effects';

it('coalesces overlaps but refreshes each collection; replicas do not increment counters',async()=>{
  let resolve!:(value:{overdue:number;integrity_error:number})=>void;
  const read=vi.fn(()=>new Promise<{overdue:number;integrity_error:number}>(done=>{resolve=done;}));
  const collect=createDeliveryHealthCollector(read);const observe=vi.fn();
  const pending=[collect({observe}),collect({observe})];await Promise.resolve();expect(read).toHaveBeenCalledTimes(1);
  resolve({overdue:3,integrity_error:1});await Promise.all(pending);
  expect(observe.mock.calls).toEqual([[3,{state:'overdue'}],[1,{state:'integrity_error'}],[0,{state:'monitor_error'}],
    [3,{state:'overdue'}],[1,{state:'integrity_error'}],[0,{state:'monitor_error'}]]);
  const reset=collect({observe});await Promise.resolve();resolve({overdue:0,integrity_error:0});await reset;
  expect(read).toHaveBeenCalledTimes(2);expect(observe.mock.calls.slice(-3)).toEqual([[0,{state:'overdue'}],[0,{state:'integrity_error'}],[0,{state:'monitor_error'}]]);
  const replica=vi.fn();await createDeliveryHealthCollector(async()=>({overdue:3,integrity_error:1}))({observe:replica});
  expect(replica.mock.calls).toEqual(observe.mock.calls.slice(0,3));
});
it('fails visibly without healthy zeros or an indefinitely cached success',async()=>{
  let fail=false;const collect=createDeliveryHealthCollector(async()=>{if(fail)throw Error('db');return{overdue:2,integrity_error:0};});
  const observe=vi.fn();await collect({observe});fail=true;observe.mockClear();await collect({observe});
  expect(observe.mock.calls).toEqual([[1,{state:'monitor_error'}]]);
  await expect(collect({observe:()=>{throw Error('exporter');}})).resolves.toBeUndefined();
});
it('registers exactly one bounded-label gauge',()=>{
  registerDeliveryHealthMetrics();registerDeliveryHealthMetrics();expect(instruments.gauge).toHaveBeenCalledTimes(1);
  expect(instruments.gauge.mock.calls[0][0]).toBe('persistio.recall.delivery_health');expect(instruments.callback).toHaveBeenCalledTimes(1);
});
it('isolates every original PR policy/delivery instrument even after repeated publisher failures',()=>{
  instruments.add.mockImplementation(()=>{throw Error('publisher');});instruments.record.mockImplementation(()=>{throw Error('publisher');});
  for(let i=0;i<2;i++)for(const name of ['memoryPolicyEventCounter','recallDeliveryCounter','recallDeliveryMissingAckCounter','globalRuleDeliveryCounter'] as const) {
    expect(()=>effects[name].add(1,{event:'test'})).not.toThrow();
  }
  expect(()=>effects.recallDurationHistogram.record(1)).not.toThrow();expect(instruments.add).toHaveBeenCalledTimes(8);
});
