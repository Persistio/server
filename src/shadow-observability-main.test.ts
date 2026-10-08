import { afterEach, beforeEach, expect, it, vi } from 'vitest';
const state=vi.hoisted(()=>({
  dispatch:vi.fn(),observe:vi.fn(),closePublisher:vi.fn(),closePool:vi.fn(),
  config:{TELEMETRY_SHADOW_ENABLED:true,CUSTOMER_METRICS_GCP_PUBSUB_TOPIC:'production',CURATOR_AUTO_RUN:false,CONTRADICTION_SCAN_ENABLED:false}
}));
vi.mock('./config',()=>({getConfig:()=>state.config}));
vi.mock('./db/client',()=>({pool:{},closePool:state.closePool}));
vi.mock('./services/shadow-observability',()=>({observeWorkerShadow:state.observe}));
vi.mock('./services/customer-metric-shadow',()=>({ShadowCustomerMetricDispatcher:class {dispatchOnce=state.dispatch;}}));
vi.mock('./services/shadow-customer-publisher',()=>({ShadowPubSubPublisher:class {close=state.closePublisher;}}));
import { main } from './shadow-observability-main';
const initialExitCode=process.exitCode;
beforeEach(()=>{
  vi.clearAllMocks();state.config.TELEMETRY_SHADOW_ENABLED=true;
  state.dispatch.mockResolvedValue({delivered:0,retried:1,stale:0,dead:0,purged:0});
  state.observe.mockResolvedValue([]);state.closePublisher.mockResolvedValue(undefined);state.closePool.mockResolvedValue(undefined);
  vi.spyOn(console,'info').mockImplementation(()=>{});
});
afterEach(()=>{vi.restoreAllMocks();process.exitCode=initialExitCode;});
it('propagates delivery failure to executable status and closes both resources',async()=>{
  await main(['--publish-shadow-topic','projects/test/topics/events-shadow']);
  expect(process.exitCode).toBe(1);expect(state.observe).toHaveBeenCalledOnce();
  expect(state.closePublisher).toHaveBeenCalledOnce();expect(state.closePool).toHaveBeenCalledOnce();
});
it('runs observation and cleanup after a thrown delivery operation',async()=>{
  state.dispatch.mockRejectedValue(new Error('PRIVATE transport failure'));await main(['--publish-shadow-topic','test']);
  expect(process.exitCode).toBe(1);expect(state.observe).toHaveBeenCalledOnce();expect(state.closePool).toHaveBeenCalledOnce();
});
it('closes SQL even if publisher cleanup fails',async()=>{
  state.closePublisher.mockRejectedValue(new Error('cleanup failed'));
  await expect(main(['--publish-shadow-topic','test'])).rejects.toThrow('cleanup failed');
  expect(state.closePool).toHaveBeenCalledOnce();
});
it('rejects disabled utility before delivery and still closes SQL',async()=>{
  state.config.TELEMETRY_SHADOW_ENABLED=false;await expect(main([])).rejects.toThrow('explicitly enabled');
  expect(state.dispatch).not.toHaveBeenCalled();expect(state.observe).not.toHaveBeenCalled();expect(state.closePool).toHaveBeenCalledOnce();
});
