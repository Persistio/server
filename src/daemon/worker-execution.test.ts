import { afterEach, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({
  port: { on: vi.fn(), postMessage: vi.fn() },
  query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
  claim: vi.fn().mockResolvedValue([]),
  activate: vi.fn().mockResolvedValue(undefined),
  extractor: vi.fn(function () {}),
  curator: vi.fn(function () {}),
  embedder: vi.fn().mockReturnValue({}),
  storage: vi.fn().mockReturnValue({}),
  initMetrics: vi.fn(),
  initCrypto: vi.fn(),
  limit: vi.fn(),
}));
vi.mock('node:worker_threads', async original => ({ ...await original<typeof import('node:worker_threads')>(), parentPort: state.port }));
vi.mock('../db/client', async original => ({ ...await original<typeof import('../db/client')>(), query: state.query }));
vi.mock('../services/curation-capacity', async original => ({ ...await original<typeof import('../services/curation-capacity')>(), claimEligibleCurationJobs: state.claim }));
vi.mock('../services/contradiction-activation', () => ({ drainDueContradictionActivations: state.activate }));
vi.mock('../services/extractor', () => ({ ExtractorService: state.extractor }));
vi.mock('../services/curator', async original => ({ ...await original<typeof import('../services/curator')>(), CuratorService: state.curator }));
vi.mock('../services/embedder', () => ({ getEmbedder: state.embedder }));
vi.mock('../services/raw-chunk-storage', () => ({ getRawChunkStorage: state.storage }));
vi.mock('../services/customer-metrics', async original => ({ ...await original<typeof import('../services/customer-metrics')>(), initCustomerMetrics: state.initMetrics }));
vi.mock('../services/crypto', async original => ({ ...await original<typeof import('../services/crypto')>(), initCryptoClient: state.initCrypto }));
vi.mock('p-limit', async original => {
  const { default: limit } = await original<typeof import('p-limit')>();
  return { default: state.limit.mockImplementation(limit) };
});

afterEach(() => vi.useRealTimers());

it('imports and constructs without starting runtime work; preserves ownership between batches', async () => {
  vi.useFakeTimers();
  const { createExtractionExecution } = await import('./extraction-execution');
  const { createCurationExecution } = await import('./curation-execution');
  expect(state.extractor).not.toHaveBeenCalled();
  expect(state.curator).not.toHaveBeenCalled();
  const extraction = createExtractionExecution();
  const otherExtraction = createExtractionExecution();
  const curation = createCurationExecution();
  const otherCuration = createCurationExecution();
  expect(state.query).not.toHaveBeenCalled();
  expect(state.claim).not.toHaveBeenCalled();
  expect(state.activate).not.toHaveBeenCalled();
  expect(state.port.on).not.toHaveBeenCalled();
  expect(state.initMetrics).not.toHaveBeenCalled();
  expect(state.initCrypto).not.toHaveBeenCalled();
  expect(state.limit.mock.calls).toEqual([[5], [5]]);
  expect(vi.getTimerCount()).toBe(0);

  expect(await extraction.processBatch()).toBe(0);
  expect(await extraction.processBatch('vault-scope')).toBe(0);
  await otherExtraction.processBatch();
  const values = state.query.mock.calls.map(call => call[1] as unknown[]);
  expect(values[0].at(-1)).toEqual(expect.any(String));
  expect(values[1][0]).toBe('vault-scope');
  expect(values[1].at(-1)).toBe(values[0].at(-1));
  expect(values[2].at(-1)).not.toBe(values[0].at(-1));
  await curation.processBatch();
  await curation.processBatch();
  await otherCuration.processBatch();
  expect(state.claim.mock.calls[0][1]).toEqual(expect.any(String));
  expect(state.claim.mock.calls[1][1]).toBe(state.claim.mock.calls[0][1]);
  expect(state.claim.mock.calls[2][1]).not.toBe(state.claim.mock.calls[0][1]);
  expect(state.extractor).toHaveBeenCalledTimes(2);
  expect(state.curator).toHaveBeenCalledTimes(2);
  expect(state.limit).toHaveBeenCalledTimes(2);
  expect(state.activate).not.toHaveBeenCalled();
  await extraction.activateDueContradictions();
  expect(state.activate).toHaveBeenCalledWith(state.extractor.mock.instances[0]);
  expect(vi.getTimerCount()).toBe(0);
});
