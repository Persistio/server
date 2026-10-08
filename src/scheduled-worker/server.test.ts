import http from 'node:http';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createScheduledWorkerServer } from './server';

const runtimes: ReturnType<typeof createScheduledWorkerServer>[] = [];
function setup(overrides: Partial<Parameters<typeof createScheduledWorkerServer>[0]> = {}) {
  const options = { authenticate: vi.fn().mockResolvedValue(true), observe: vi.fn().mockResolvedValue(0),
    reconcile: vi.fn().mockResolvedValue(undefined), fatal: vi.fn(), ...overrides };
  const runtime = createScheduledWorkerServer(options);
  runtimes.push(runtime);
  return { ...runtime, options };
}
afterEach(async () => { await Promise.all(runtimes.splice(0).map(r => r.stop())); });
const call = (app: ReturnType<typeof setup>['app'], path = 'observe', payload?: unknown) =>
  app.inject({ method: 'POST', url: `/internal/${path}`, ...(payload === undefined ? {} : { payload }) });

describe('scheduled foundation host', () => {
  it('rejects forged auth before observation and exposes no worker execution or API route', async () => {
    const r = setup({ authenticate: vi.fn().mockResolvedValue(false) });
    const response = await r.app.inject({ method: 'POST', url: '/internal/observe',
      headers: { 'x-cloudscheduler': 'true', 'x-api-key': 'admin', authorization: 'Bearer forged' } });
    expect(response.statusCode).toBe(401);
    expect(r.options.observe).not.toHaveBeenCalled();
    expect(r.options.reconcile).not.toHaveBeenCalled();
    for (const path of ['/internal/worker/wake', '/internal/dispatch', '/v1/ingest']) {
      expect((await r.app.inject({ method: 'POST', url: path })).statusCode).toBe(404);
    }
  });
  it('awaits full observation and cleanup before acknowledgement and rejects overlap', async () => {
    let release!: () => void;
    let started!: () => void;
    const startedPromise = new Promise<void>(r => { started = r; });
    const r = setup({ observe: vi.fn(async () => {
      started(); await new Promise<void>(resolve => { release = resolve; }); return 0 as const;
    }) });
    const first = call(r.app).then(response => response);
    await startedPromise;
    expect((await call(r.app)).statusCode).toBe(503);
    expect((await call(r.app, 'reconcile')).statusCode).toBe(503);
    release();
    expect((await first).statusCode).toBe(204);
    expect(r.options.observe).toHaveBeenCalledOnce();
    expect(r.options.reconcile).not.toHaveBeenCalled();
  });
  it('propagates reported delivery failures, thrown errors and auth failures without private error text', async () => {
    const observe = vi.fn().mockResolvedValueOnce(1).mockRejectedValueOnce(new Error('private-canary'));
    const r = setup({ observe });
    for (let i = 0; i < 2; i++) {
      const response = await call(r.app);
      expect(response.statusCode).toBe(503);
      expect(response.body).not.toContain('private-canary');
    }
    expect((await call(r.app, 'reconcile')).statusCode).toBe(204);
    expect(r.options.reconcile).toHaveBeenCalledOnce();
  });
  it('rejects input targets and oversized bodies', async () => {
    const r = setup();
    expect((await call(r.app, 'observe', { topic: 'production' })).statusCode).toBe(400);
    expect((await call(r.app, 'observe', { data: 'x'.repeat(2048) })).statusCode).toBe(413);
    expect(r.options.observe).not.toHaveBeenCalled();
  });
  it('terminates if the caller disconnects during active work', async () => {
    let release!: () => void;
    let started!: () => void;
    let terminated!: () => void;
    const ready = new Promise<void>(resolve => { started = resolve; });
    const stopped = new Promise<void>(resolve => { terminated = resolve; });
    const r = setup({ fatal: vi.fn(() => terminated()), observe: vi.fn(async () => {
      started(); await new Promise<void>(resolve => { release = resolve; }); return 0 as const;
    }) });
    const origin = await r.app.listen({ host: '127.0.0.1', port: 0 });
    const request = http.request(origin + '/internal/observe', { method: 'POST' });
    request.on('error', () => {});
    request.end();
    await ready;
    request.destroy();
    await stopped;
    expect(r.options.fatal).toHaveBeenCalledOnce();
    release();
    expect((await r.app.inject('/health')).statusCode).toBe(503);
  });
  it('terminates on hard deadline and never acknowledges a late successful operation', async () => {
    let release!: () => void;
    let terminated!: () => void;
    const termination = new Promise<void>(r => { terminated = r; });
    const fatal = vi.fn(() => { terminated(); });
    const r = setup({ deadlineMs: 20, fatal, observe: vi.fn(() => new Promise<0>(resolve => { release = () => resolve(0); })) });
    const first = call(r.app).then(response => response);
    await termination;
    expect(fatal).toHaveBeenCalledOnce();
    expect((await r.app.inject('/health')).statusCode).toBe(503);
    release();
    expect((await first).statusCode).toBe(503);
    expect((await call(r.app)).statusCode).toBe(503);
  });
});
