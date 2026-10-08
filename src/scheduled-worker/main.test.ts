import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({
  config: { DATABASE_URL: 'postgresql://unused', PORT: 4827, TELEMETRY_SHADOW_ENABLED: true,
    TELEMETRY_PROVIDER: 'none', CUSTOMER_METRICS_PUBLISHER: 'noop', CUSTOMER_METRICS_GCP_PUBSUB_TOPIC: 'production' },
  wakeEnabled: false, wakeDispatch: vi.fn(), query: vi.fn(), listen: vi.fn(), close: vi.fn(), dueAt: vi.fn(), observe: vi.fn(), dispatch: vi.fn(),
  publisherClose: vi.fn(), serverOptions: undefined as any, poolOptions: undefined as any
}));
vi.mock('pg', () => ({ Pool: class {
  constructor(options: unknown) { state.poolOptions = options; }
  query = state.query; on = vi.fn(); end = state.close;
} }));
vi.mock('../config', () => ({ getConfig: () => state.config }));
vi.mock('../request-worker/config', () => ({ requestWorkerConfig: () => ({}) }));
vi.mock('../request-worker/auth', () => ({ createWorkerAuthenticator: () => vi.fn() }));
vi.mock('../services/shadow-customer-publisher', () => ({ ShadowPubSubPublisher: class { close = state.publisherClose; }, ProductionCustomerPublisher: class { close = state.publisherClose; } }));
vi.mock('../services/customer-metric-shadow', () => ({ ShadowCustomerMetricDispatcher: class { dispatchOnce = state.dispatch; } }));
vi.mock('../services/shadow-observability', () => ({ observeWorkerShadow: state.observe }));
vi.mock('../services/worker-wake-reconciliation', () => ({ WorkerWakeReconciliation: class { dueAt = state.dueAt; } }));
vi.mock('../services/worker-wake-runtime', () => ({configuredWakeDispatcher: () => state.wakeEnabled ? {dispatchOnce:state.wakeDispatch} : undefined}));
vi.mock('./server', () => ({ ScheduledCleanupError: class extends Error {}, createScheduledWorkerServer: (options: unknown) => {
  state.serverOptions = options;
  return { app: { listen: state.listen }, stop: vi.fn() };
} }));
import { startScheduledWorker } from './main';
let handlers: Map<NodeJS.Signals, Function[]>;
beforeEach(() => {
  vi.clearAllMocks();
  state.wakeEnabled = false;
  state.wakeDispatch.mockResolvedValue({failures:{}});
  handlers = new Map(['SIGTERM', 'SIGINT'].map(s => [s as NodeJS.Signals, process.listeners(s)]));
  vi.stubEnv('OBSERVE_CURATION_ENABLED', 'true'); vi.stubEnv('OBSERVE_CONTRADICTION_ENABLED', 'false');
  state.config.TELEMETRY_SHADOW_ENABLED = true;
  state.query.mockResolvedValue({ rows: [] }); state.publisherClose.mockResolvedValue(undefined);
  state.observe.mockResolvedValue([]); state.dispatch.mockResolvedValue({ delivered: 0, retried: 0, stale: 0, dead: 0, purged: 0 });
});
afterEach(() => {
  vi.unstubAllEnvs();
  for (const [signal, previous] of handlers) for (const handler of process.listeners(signal)) {
    if (!previous.includes(handler)) process.removeListener(signal, handler);
  }
});
it('verifies schema without migrations and constructs a single bounded connection pool', async () => {
  await startScheduledWorker();
  expect(state.poolOptions).toMatchObject({ max: 1, connectionTimeoutMillis: 3000, statement_timeout: 5000, lock_timeout: 1000 });
  expect(state.query.mock.calls.every(([sql]) => sql.startsWith('SELECT '))).toBe(true);
  expect(state.query).toHaveBeenCalledTimes(3);
  expect(state.listen).toHaveBeenCalledOnce();
  expect(state.dispatch).not.toHaveBeenCalled();
  expect(state.observe).not.toHaveBeenCalled();
});
it('preserves delivery failure and closes publisher before returning the request result', async () => {
  await startScheduledWorker();
  state.dispatch.mockRejectedValue(new Error('private transport error'));
  expect(await state.serverOptions.observe()).toBe(1);
  expect(state.observe).toHaveBeenCalledWith(expect.anything(), { enabled: true, productionMetrics: false, curationEnabled: true, contradictionEnabled: false });
  expect(state.publisherClose).toHaveBeenCalledTimes(2);
});
it('cleanup failure makes a successful pass fail and reconciliation never calls delivery', async () => {
  await startScheduledWorker();
  state.publisherClose.mockRejectedValueOnce(new Error('cleanup failed'));
  await expect(state.serverOptions.observe()).rejects.toThrow('Publisher cleanup failed');
  state.dispatch.mockClear(); state.query.mockClear();
  await state.serverOptions.reconcile();
  expect(state.dueAt).toHaveBeenCalledTimes(6);
  expect(state.dispatch).not.toHaveBeenCalled();
  expect(state.query).not.toHaveBeenCalled();
});
it('fails before listen when schema is missing or observation is disabled', async () => {
  state.query.mockRejectedValueOnce(new Error('missing relation'));
  await expect(startScheduledWorker()).rejects.toThrow('missing relation');
  expect(state.listen).not.toHaveBeenCalled();
  state.config.TELEMETRY_SHADOW_ENABLED = false;
  await expect(startScheduledWorker()).rejects.toThrow('telemetry');
});

it('awaits production delivery and wake dispatch even when its shadow sibling fails', async () => {
  vi.stubEnv('CUSTOMER_METRICS_DURABLE_DELIVERY', 'true');
  state.wakeEnabled = true;
  await startScheduledWorker();
  expect(state.query).toHaveBeenCalledWith('SELECT event_id FROM customer_metric_outbox LIMIT 0');
  let finishDelivery!: () => void, finishWake!: () => void;
  state.dispatch.mockRejectedValueOnce(new Error('shadow unavailable'));
  state.dispatch.mockImplementationOnce(() => new Promise<void>(resolve => {finishDelivery=() => resolve(undefined);}));
  state.wakeDispatch.mockImplementationOnce(() => new Promise(resolve => {finishWake=() => resolve({failures:{}});}));
  let returned = false;
  const pass = state.serverOptions.observe().then((result: number) => {returned=true;return result;});
  await vi.waitFor(() => expect(state.wakeDispatch).toHaveBeenCalledOnce());
  finishWake(); await Promise.resolve(); expect(returned).toBe(false);
  finishDelivery(); expect(await pass).toBe(1);
  expect(state.publisherClose).toHaveBeenCalledTimes(4);
  expect(state.observe).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({productionMetrics:true}));
});
