import { afterEach, describe, expect, it, vi } from 'vitest';
import { meter } from './telemetry';
import { getConfig } from './config';
import { operationalMetadata } from './operational-metadata';
import { shadowCounter, shadowHistogram, SHADOW_METRIC_NAMES } from './shadow-metrics';
import { shadowWorkerCycle } from './services/shadow-observability';

afterEach(()=>{getConfig().TELEMETRY_SHADOW_ENABLED=false;vi.restoreAllMocks();});
describe('actual shadow exports',()=>{
  it('is disabled by default and never leaks tenant/content/error canaries when enabled',()=>{
    const info=vi.spyOn(console,'info').mockImplementation(()=>{});
    const counter=shadowCounter('persistio.extraction.jobs.total',{});
    counter.add(1,{status:'success'});expect(info).not.toHaveBeenCalled();
    getConfig().TELEMETRY_SHADOW_ENABLED=true;
    counter.add(2,{status:'success',vault_id:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',model:'PRIVATE_MODEL',query:'PRIVATE_QUERY',outcome:'PRIVATE_OUTCOME'});
    const record=JSON.parse(String(info.mock.calls[0][0]));
    expect(record).toMatchObject({event:'shadow_domain_metric',value:2,status:'success',shadow_metric:'persistio.extraction.jobs.total'});
    expect(JSON.stringify(record)).not.toContain('PRIVATE');expect(record).not.toHaveProperty('vault_id');
    expect(operationalMetadata(operationalMetadata(record))).toEqual(operationalMetadata(record));
    for(const name of SHADOW_METRIC_NAMES)expect(operationalMetadata({shadow_metric:name}).shadow_metric).toBe(name);
  });
  it.each(['extraction','escalation','curation'])('retains %s role in both budget instruments without changing OTel attributes',role=>{
    const add=vi.fn(),record=vi.fn();
    vi.spyOn(meter,'createCounter').mockReturnValue({add});
    vi.spyOn(meter,'createHistogram').mockReturnValue({record});
    const info=vi.spyOn(console,'info').mockImplementation(()=>{});
    const counter=shadowCounter('persistio.ai_budget.throttled_jobs.total',{});
    const histogram=shadowHistogram('persistio.ai_budget.wait_ms',{});
    const attributes=Object.freeze({role,queue:role==='curation'?'curation':'extraction',
      vault_id:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',model:'PRIVATE_MODEL'});
    counter.add(1,attributes);histogram.record(60000,attributes);
    expect(info).not.toHaveBeenCalled();
    getConfig().TELEMETRY_SHADOW_ENABLED=true;
    counter.add(1,attributes);histogram.record(60000,attributes);
    const exported=info.mock.calls.map(([line])=>JSON.parse(String(line)));
    expect(exported).toHaveLength(2);
    for(const entry of exported){
      expect(entry).toMatchObject({event:'shadow_domain_metric',model_role:role,queue:attributes.queue});
      expect(entry).not.toHaveProperty('role');expect(entry).not.toHaveProperty('vault_id');
      expect(JSON.stringify(entry)).not.toContain('PRIVATE');
    }
    expect(exported.map(entry=>[entry.shadow_metric,entry.value])).toEqual([
      ['persistio.ai_budget.throttled_jobs.total',1],['persistio.ai_budget.wait_ms',60000]
    ]);
    for(const spy of [add,record]){
      expect(spy).toHaveBeenCalledTimes(2);
      expect(spy.mock.calls.every(call=>call[1]===attributes)).toBe(true);
    }
  });
  it.each([
    [{model_role:'curation',role:'extraction'},'curation'],
    [{model_role:'embedding'},'embedding'],
    [{model_role:'PRIVATE',role:'extraction'},undefined],
    [{role:'PRIVATE'},undefined], [{},undefined]
  ])('normalizes role keys without widening the allowlist: %j',(attributes,expected)=>{
    getConfig().TELEMETRY_SHADOW_ENABLED=true;
    const info=vi.spyOn(console,'info').mockImplementation(()=>{});
    shadowCounter('persistio.ai_budget.throttled_jobs.total',{}).add(1,attributes);
    shadowHistogram('persistio.ai_budget.wait_ms',{}).record(10,attributes);
    expect(info).toHaveBeenCalledTimes(2);
    for(const [line] of info.mock.calls){
      const entry=JSON.parse(String(line));
      if(expected===undefined)expect(entry).not.toHaveProperty('model_role');
      else expect(entry.model_role).toBe(expected);
      expect(JSON.stringify(entry)).not.toContain('PRIVATE');
    }
  });
  it('preserves bounded outcomes and timings without letting a broken log sink fail business work',()=>{
    getConfig().TELEMETRY_SHADOW_ENABLED=true;
    const info=vi.spyOn(console,'info').mockImplementation(()=>{});
    shadowWorkerCycle('extraction','completed',123);
    expect(JSON.parse(String(info.mock.calls[0][0]))).toMatchObject({event:'shadow_worker_cycle',shadow_kind:'extraction',shadow_outcome:'completed',duration_ms:123});
    info.mockImplementation(()=>{throw new Error('PRIVATE');});
    expect(()=>shadowWorkerCycle('extraction','failed',123)).not.toThrow();
    expect(()=>shadowHistogram('persistio.recall.duration',{}).record(100,{mode:'agent'})).not.toThrow();
    expect(()=>shadowCounter('persistio.ai_budget.throttled_jobs.total',{}).add(1,{role:'extraction'})).not.toThrow();
    expect(()=>shadowHistogram('persistio.ai_budget.wait_ms',{}).record(100,{role:'curation'})).not.toThrow();
  });
  it('drops unknown instrument values and observation keys through the real sanitizer',()=>{
    expect(operationalMetadata({shadow_metric:'PRIVATE',shadow_kind:'PRIVATE',shadow_health:'PRIVATE',
      payload:{query:'PRIVATE'},observed_at_ms:1,eligible_depth:0,last_cycle_success_age_ms:Infinity}))
      .toEqual({observed_at_ms:1,eligible_depth:0});
  });
});
