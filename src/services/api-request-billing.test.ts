import Fastify from 'fastify';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { getConfig } from '../config';
import { registerCustomerRequestObservations } from './customer-api-request-metrics';
import { initCustomerMetrics, recordCustomerMetric, shutdownCustomerMetrics } from './customer-metrics';
import { RawChunkWorkPool } from './raw-chunk-work-pool';
import { requestAll } from './request-work';

const { capture, auxiliary } = vi.hoisted(() => ({ capture: vi.fn(), auxiliary: vi.fn() }));
vi.mock('./customer-metric-shadow', () => ({ captureShadowCustomerMetric: capture }));
vi.mock('../db/auxiliary', () => ({ withAuxiliaryClient: auxiliary }));
afterEach(async () => { vi.useRealTimers(); vi.resetAllMocks(); getConfig().API_REQUEST_BILLING = false; await shutdownCustomerMetrics(); });

async function app() {
  const app = Fastify();
  registerCustomerRequestObservations(app, true);
  app.addHook('preHandler', async request => { request.auth = { account_id: 'workspace', method: 'oauth' } as any; });
  app.get('/v1/memories', async () => ({ ok: true }));
  app.post('/v1/recall', async (_request, reply) => reply.code(429).send({ error: 'limited' }));
  app.get('/v1/memories/:id', async () => { throw new Error('handler failure'); });
  auxiliary.mockImplementation(work => work({}));
  return app;
}

describe('guarded API response ownership', () => {
  it('does not send until durable capture settles; emits a versioned pre-response observation once', async () => {
    const api = await app(); let release!: () => void; let arrived!: () => void;
    const entered = new Promise<void>(resolve => { arrived = resolve; });
    capture.mockImplementation(() => { arrived(); return new Promise<void>(resolve => { release = resolve; }); });
    let sent = false;
    const response = api.inject('/v1/memories').then(r => { sent = true; return r; });
    await entered;
    expect(sent).toBe(false);
    release(); expect((await response).statusCode).toBe(200);
    expect(capture).toHaveBeenCalledTimes(1);
    expect(capture.mock.calls[0][1]).toMatchObject({ event_type: 'api_request', api_request_count: 1,
      contract_version: 1, labels: { observation_phase: 'pre_response', request_observation_version: 2 }, status_code: 200 });
    await api.close();
  });
  it('preserves successful, early and error responses on capture failure without retrying', async () => {
    const api = await app(); capture.mockRejectedValue(new Error('private SQL error'));
    expect((await api.inject('/v1/memories')).json()).toEqual({ ok: true });
    expect((await api.inject({ method: 'POST', url: '/v1/recall' })).statusCode).toBe(429);
    expect((await api.inject('/v1/memories/id')).statusCode).toBe(500);
    expect(capture.mock.calls.map(call => call[1].status_code)).toEqual([200, 429, 500]);
    expect((await api.inject('/health')).statusCode).toBe(404);
    expect(capture).toHaveBeenCalledTimes(3);
    await api.close();
  });
  it('never creates a noop buffer or flush timer', async () => {
    vi.useFakeTimers(); await initCustomerMetrics(getConfig());
    recordCustomerMetric({ event_type: 'api_request', api_request_count: 1, duration_ms: 1, method: 'GET', operation: 'test', route: '/test', source: 'api', status_code: 200, workspace_id: 'w' });
    expect(vi.getTimerCount()).toBe(0);
  });
  it('waits for sibling work after failure and preserves the original failure', async () => {
    getConfig().API_REQUEST_BILLING = true;
    let release!: () => void; let settled = false;
    const sibling = new Promise<void>(resolve => { release = resolve; });
    const work = requestAll([Promise.reject(new Error('original')), sibling]).catch(e => { settled = true; return e; });
    await new Promise(resolve => setImmediate(resolve)); expect(settled).toBe(false);
    release(); expect((await work).message).toBe('original');
  });
  it('cancels admission at deadline but holds the response until actual upload settlement', async () => {
    const pool = new RawChunkWorkPool(1, { required: () => true });
    let release!: () => void; let signal: AbortSignal | undefined; let settled = false; const started: number[] = [];
    const work = pool.map([1, 2], async (n, _i, s) => { signal = s; started.push(n); await new Promise<void>(r => { release = r; }); return n; }, 5)
      .catch(e => { settled = true; return e; });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(signal?.aborted).toBe(true); expect(settled).toBe(false); expect(started).toEqual([1]);
    release(); expect((await work).message).toContain('deadline');
    expect(started).toEqual([1]); expect(await pool.map([3], async n => n)).toEqual([3]);
  });
});
