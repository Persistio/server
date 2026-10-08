import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({
  listener: undefined as undefined | ((message: unknown) => void),
  post: vi.fn(), close: vi.fn(),
  batch: vi.fn(), activate: vi.fn(),
  init: vi.fn(), publishers: vi.fn(), telemetry: vi.fn(), pool: vi.fn(),
  factory: vi.fn(),
}));
vi.mock('node:worker_threads', () => ({ parentPort: {
  on: (_event: string, listener: (message: unknown) => void) => { state.listener = listener; },
  postMessage: state.post, close: state.close,
} }));
vi.mock('../config', () => ({ getConfig: () => ({ ENCRYPTION_ENABLED: false, EXTRACTION_INTERVAL_MS: 60_000, CURATION_INTERVAL_MS: 60_000 }) }));
vi.mock('../db/client', () => ({ closePool: state.pool }));
vi.mock('../services/crypto', () => ({ initCryptoClient: vi.fn() }));
vi.mock('../services/customer-metrics', () => ({ initCustomerMetrics: state.init, shutdownCustomerMetrics: state.publishers }));
vi.mock('../azure-monitor', () => ({ shutdownTelemetry: state.telemetry }));
vi.mock('../telemetry', () => ({ getSpanAttributes: () => ({}) }));
vi.mock('../metrics', () => ({ extractionJobsCounter: { add: vi.fn() } }));
vi.mock('./extraction-execution', () => ({ createExtractionExecution: state.factory }));
vi.mock('./curation-execution', () => ({ createCurationExecution: state.factory }));

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
async function flush() { for (let i = 0; i < 40; i++) await Promise.resolve(); }

beforeEach(() => {
  vi.resetModules();
  vi.resetAllMocks();
  vi.stubEnv('K_SERVICE', '');
  vi.useFakeTimers();
  state.listener = undefined;
  state.batch.mockResolvedValue(0);
  state.activate.mockResolvedValue(undefined);
  state.factory.mockReturnValue({ processBatch: state.batch, activateDueContradictions: state.activate });
});
afterEach(() => { vi.useRealTimers(); vi.unstubAllEnvs(); });

it('keeps scheduled activation separate from manual runs and drains manual status before cleanup', async () => {
  const initial = deferred();
  state.batch.mockReturnValueOnce(initial.promise);
  await import('./extraction-worker');
  await flush();
  expect(state.init).toHaveBeenCalledOnce();
  expect(state.batch).toHaveBeenCalledExactlyOnceWith();
  expect(state.activate).not.toHaveBeenCalled();
  initial.resolve();
  await flush();
  expect(state.activate).toHaveBeenCalledOnce();
  const manual = deferred();
  state.batch.mockReturnValueOnce(manual.promise);
  state.listener!({ type: 'run-once', jobId: 'manual', vaultId: 'vault' });
  await flush();
  expect(state.batch).toHaveBeenLastCalledWith('vault');
  expect(state.post).toHaveBeenCalledWith({ type: 'job-status', jobId: 'manual', status: 'running' });
  state.listener!({ type: 'shutdown' });
  state.listener!({ type: 'run-once', jobId: 'late' });
  await flush();
  expect(state.post).toHaveBeenCalledWith(expect.objectContaining({ jobId: 'late', status: 'failed' }));
  expect(state.pool).not.toHaveBeenCalled();
  expect(state.close).not.toHaveBeenCalled();
  expect(state.batch).toHaveBeenCalledTimes(2);
  manual.resolve();
  await flush();
  expect(state.post.mock.calls.map(([message]) => message)).toEqual([
    { type: 'job-status', jobId: 'manual', status: 'running' },
    expect.objectContaining({ jobId: 'late', status: 'failed' }),
    { type: 'job-status', jobId: 'manual', status: 'completed' },
    { type: 'shutdown-complete' },
  ]);
  expect(state.activate).toHaveBeenCalledOnce();
  expect(state.factory).toHaveBeenCalledOnce();
  expect(state.publishers.mock.invocationCallOrder[0]).toBeLessThan(state.telemetry.mock.invocationCallOrder[0]);
  expect(state.telemetry.mock.invocationCallOrder[0]).toBeLessThan(state.pool.mock.invocationCallOrder[0]);
  expect(state.pool.mock.invocationCallOrder[0]).toBeLessThan(state.close.mock.invocationCallOrder[0]);
  expect(vi.getTimerCount()).toBe(0);
});

it.each(['extraction', 'curation'])('%s drains a held scheduled batch before acknowledging shutdown', async kind => {
  const held = deferred();
  state.batch.mockReturnValueOnce(held.promise);
  if (kind === 'extraction') await import('./extraction-worker');
  else await import('./curation-worker');
  await flush();
  expect(state.batch).toHaveBeenCalledOnce();
  state.listener!({ type: 'shutdown' });
  await flush();
  expect(state.publishers).not.toHaveBeenCalled();
  expect(state.close).not.toHaveBeenCalled();
  held.resolve();
  await flush();
  expect(state.activate).not.toHaveBeenCalled();
  expect(state.post).toHaveBeenCalledWith({ type: 'shutdown-complete' });
  expect(state.pool).toHaveBeenCalledOnce();
  expect(state.close).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(120_000);
  expect(state.batch).toHaveBeenCalledOnce();
});

it.each(['extraction', 'curation'])('%s reuses its execution owner across polling intervals', async kind => {
  if (kind === 'extraction') await import('./extraction-worker');
  else await import('./curation-worker');
  await flush();
  expect(state.batch).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(59_999);
  expect(state.batch).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(1);
  expect(state.batch).toHaveBeenCalledTimes(2);
  expect(state.factory).toHaveBeenCalledOnce();
  expect(state.activate).toHaveBeenCalledTimes(kind === 'extraction' ? 2 : 0);
  state.listener!({ type: 'shutdown' });
  await flush();
  expect(state.close).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});
