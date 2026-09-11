import { EventEmitter } from 'node:events';
import type { Worker } from 'node:worker_threads';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { closeRuntimeHttp, createRuntimeShutdown, createShutdownDeadline, parseShutdownDeadline, drainRuntimeOwner, stopRuntimeWorker } from './runtime-shutdown';
import Fastify from 'fastify';
import { PassThrough } from 'node:stream';
import { CustomerMetricEmitter, GcpPubSubCustomerMetricPublisher } from './services/customer-metrics';
import { GcpPubSubEventPublisher } from './events/event-publisher-gcp-pubsub';

const deferred = () => { let resolve!: () => void; let reject!: (e: Error) => void;
  const promise = new Promise<void>((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
describe('complete runtime shutdown lifecycle', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
  it('starts all owners together and repeated signals do not restart cleanup', async () => {
    const exit = vi.fn(); const owners = [deferred(), deferred(), deferred()];
    const starts = owners.map(owner => vi.fn(() => owner.promise));
    const shutdown = createRuntimeShutdown({ branches: starts, cloudRun: true, exit, warn: vi.fn() });
    const first = shutdown(); expect(shutdown()).toBe(first);
    await vi.advanceTimersByTimeAsync(0);
    starts.forEach(start => expect(start).toHaveBeenCalledTimes(1));
    owners[0].resolve(); owners[1].resolve(); await vi.advanceTimersByTimeAsync(5000);
    expect(exit).not.toHaveBeenCalled(); owners[2].resolve(); await first;
    expect(exit).toHaveBeenCalledExactlyOnceWith(0); expect(vi.getTimerCount()).toBe(0);
  });
  it('three five-second flushes consume five seconds, not fifteen', async () => {
    const exit = vi.fn();
    const flush = () => new Promise<void>(resolve => setTimeout(resolve, 5000));
    const shutdown = createRuntimeShutdown({ branches: [flush, flush, flush], cloudRun: true, exit, warn: vi.fn() });
    const done = shutdown(); await vi.advanceTimersByTimeAsync(4999); expect(exit).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); await done; expect(exit).toHaveBeenCalledExactlyOnceWith(0);
  });
  it('bounds hung work from the first signal and never reports it as drained', async () => {
    const exit = vi.fn(); const work = deferred(); const pool = vi.fn();
    const shutdown = createRuntimeShutdown({ cloudRun: true, exit,
      warn: () => { throw new Error('diagnostic unavailable'); },
      branches: [() => drainRuntimeOwner({ drain: [() => work.promise], publishers: [], telemetry: vi.fn(), pool })] });
    const done = shutdown(); await vi.advanceTimersByTimeAsync(8000); expect(shutdown()).toBe(done);
    await vi.advanceTimersByTimeAsync(1000); await done;
    expect(exit).toHaveBeenCalledExactlyOnceWith(1); expect(pool).not.toHaveBeenCalled();
    work.reject(new Error('late rejection')); await vi.advanceTimersByTimeAsync(1);
    expect(exit).toHaveBeenCalledTimes(1);
  });
  it('non-Cloud hosts retain their existing grace and branch rejection does not skip other cleanup', async () => {
    const slow = deferred(); const clean = vi.fn(); const exit = vi.fn();
    const done = createRuntimeShutdown({ cloudRun: false, exit, warn: vi.fn(),
      branches: [() => { throw new Error('failed'); }, async () => { await slow.promise; clean(); }] })();
    await vi.advanceTimersByTimeAsync(11000); expect(exit).not.toHaveBeenCalled();
    slow.resolve(); await done; expect(clean).toHaveBeenCalledOnce(); expect(exit).toHaveBeenCalledWith(1);
  });
  it('retains resources through active work and telemetry, even if a publisher rejects', async () => {
    const work = deferred(); const telemetry = deferred(); const calls: string[] = [];
    const result = drainRuntimeOwner({ drain: [() => work.promise, async () => { calls.push('http-stopped'); }],
      publishers: [async () => { calls.push('publisher'); throw new Error('failed'); }],
      telemetry: () => { calls.push('telemetry'); return telemetry.promise; }, pool: async () => { calls.push('pool'); } });
    const checked = expect(result).rejects.toThrow('Runtime cleanup incomplete');
    await vi.advanceTimersByTimeAsync(0); expect(calls).toEqual(['http-stopped']);
    work.resolve(); await vi.advanceTimersByTimeAsync(0); expect(calls).toEqual(['http-stopped', 'publisher', 'telemetry']);
    telemetry.resolve(); await checked; expect(calls.at(-1)).toBe('pool');
  });
  it('waits for every main producer after a drain failure and never closes underneath work', async () => {
    const work = deferred(); const pool = vi.fn(); const publisher = vi.fn();
    const result = drainRuntimeOwner({ drain: [() => { throw new Error('stop failed'); }, () => work.promise],
      publishers: [publisher], telemetry: vi.fn(), pool });
    const checked = expect(result).rejects.toThrow('Runtime work drain failed');
    await vi.advanceTimersByTimeAsync(5000); expect(publisher).not.toHaveBeenCalled();
    work.resolve(); await checked; expect(pool).not.toHaveBeenCalled();
  });
});

describe('actual HTTP transport drain', () => {
  it.each(['held-handler', 'streaming-response'])('closes only idle sockets, preserving %s until completion', async scenario => {
    const app = Fastify(); const gate = deferred(); const admitted = deferred(); const body = new PassThrough();
    app.get('/fixture', async (_request, reply) => {
      admitted.resolve();
      if (scenario === 'streaming-response') return reply.send(body);
      await gate.promise; return { ok: true };
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    const address = app.server.address() as { port: number };
    const request = fetch(`http://127.0.0.1:${address.port}/fixture`);
    try {
      await admitted.promise;
      let closed = false;
      const done = closeRuntimeHttp(() => app.close(), () => app.server.closeIdleConnections()).then(() => { closed = true; });
      await new Promise(r => setTimeout(r, 250));
      expect(closed).toBe(false);
      if (scenario === 'streaming-response') body.end('complete'); else gate.resolve();
      expect(await (await request).text()).toBe(scenario === 'streaming-response' ? 'complete' : '{"ok":true}');
      await done; expect(closed).toBe(true);
    } finally {
      gate.resolve(); body.end(); app.server.closeAllConnections(); await app.close();
    }
  });
  it('observes sweep errors and clears timers after close settles or rejects', async () => {
    vi.useFakeTimers();
    try {
      const gate = deferred();
      const done = expect(closeRuntimeHttp(() => gate.promise, () => { throw new Error('idle close failed'); })).rejects.toThrow('HTTP idle cleanup failed');
      await vi.advanceTimersByTimeAsync(200); gate.resolve(); await done;
      expect(vi.getTimerCount()).toBe(0);
      await expect(closeRuntimeHttp(async () => { throw new Error('close failed'); }, vi.fn())).rejects.toThrow('close failed');
      expect(vi.getTimerCount()).toBe(0);
    } finally { vi.useRealTimers(); }
  });
});

describe('publisher/SDK dependency cross-product', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());
  for (const owner of ['main', 'extraction', 'curation']) {
    for (const phase of ['queued-publish', 'active-publish', 'customer-close', ...(owner === 'main' ? ['platform-close'] : [])]) {
      it.each(['hang', 'reject', 'slow'])(`${owner} ${phase} %s cannot starve independent final telemetry`, async failure => {
        const blocked = deferred(); const trace: string[] = [];
        const fault = vi.fn(() => {
          trace.push('publisher-start');
          if (failure === 'reject') return Promise.reject(new Error('transport failure'));
          if (failure === 'slow') return new Promise<void>(r => setTimeout(r, 6000));
          return blocked.promise;
        });
        const client = { topic: () => ({ publishMessage: async () => {
          if (phase.includes('publish')) await fault(); return 'synthetic-id';
        } }), close: async () => { if (phase === 'customer-close') await fault(); } };
        const emitter = new CustomerMetricEmitter(new GcpPubSubCustomerMetricPublisher(client, 'fixture'),
          { batchSize: 10, flushIntervalMs: 60000 });
        emitter.record({ event_type: 'api_request', api_request_count: 1, duration_ms: 0, method: 'GET',
          operation: 'fixture', route: '/fixture', status_code: 200, source: 'api', workspace_id: 'fixture' });
        if (phase === 'active-publish') void emitter.flush();
        const platform = new GcpPubSubEventPublisher({ topic: () => ({ publishMessage: async () => 'fixture' }),
          close: async () => { if (phase === 'platform-close') await fault(); } }, 'fixture');
        const exit = vi.fn(); const pool = vi.fn(async () => { trace.push('pool'); });
        const shutdown = createRuntimeShutdown({ cloudRun: true, exit, warn: vi.fn(), branches: [deadline => drainRuntimeOwner({
          drain: [], publishers: [() => emitter.close(), () => platform.close()],
          telemetry: async () => { trace.push('telemetry'); }, pool, deadline
        })] });
        const done = shutdown(); await vi.advanceTimersByTimeAsync(1);
        expect(trace).toContain('telemetry'); expect(pool).toHaveBeenCalledOnce();
        await vi.advanceTimersByTimeAsync(9000); await done;
        // The emitter intentionally handles publish rejection internally; client
        // close rejection still propagates. Neither certifies remote delivery.
        const failed = failure === 'hang' || (failure === 'reject' && phase.endsWith('close'));
        expect(exit).toHaveBeenCalledExactlyOnceWith(failed ? 1 : 0);
        if (failure === 'hang') blocked.reject(new Error('late transport rejection'));
        await vi.advanceTimersByTimeAsync(1);
        expect(exit).toHaveBeenCalledTimes(1);
      });
    }
  }
  it('stalled SDK is not treated as cancelled and does not close its pool', async () => {
    const sdk = deferred(); const pool = vi.fn(); const publisher = vi.fn(); const exit = vi.fn();
    const done = createRuntimeShutdown({ cloudRun: true, exit, warn: vi.fn(), branches: [deadline => drainRuntimeOwner({
      drain: [], publishers: [publisher], telemetry: () => sdk.promise, pool, deadline
    })] })();
    await vi.advanceTimersByTimeAsync(8001); await done;
    expect(publisher).toHaveBeenCalledOnce(); expect(pool).not.toHaveBeenCalled();
    expect(exit).toHaveBeenCalledWith(1);
    sdk.reject(new Error('late SDK failure')); await vi.advanceTimersByTimeAsync(1);
    expect(pool).not.toHaveBeenCalled();
  });
  it('keeps host ordering outside Cloud Run and awaits the real slow SDK', async () => {
    const publisher = deferred(); const sdk = deferred(); const telemetry = vi.fn(() => sdk.promise); const pool = vi.fn();
    const done = drainRuntimeOwner({ drain: [], publishers: [() => publisher.promise], telemetry, pool });
    await vi.advanceTimersByTimeAsync(6000); expect(telemetry).not.toHaveBeenCalled();
    publisher.resolve(); await vi.advanceTimersByTimeAsync(6000); expect(pool).not.toHaveBeenCalled();
    sdk.resolve(); await done; expect(pool).toHaveBeenCalledOnce();
  });
  it('passes absolute deadlines unchanged and rejects malformed internal deadlines', async () => {
    const deadline = createShutdownDeadline(true)!;
    await vi.advanceTimersByTimeAsync(5000);
    expect(parseShutdownDeadline(deadline)).toEqual(deadline);
    for (const invalid of [null, {}, { io: 1, process: 2 }, { io: 1n, process: 2n },
      { io: deadline.io + 9000_000_000n, process: deadline.process + 9000_000_000n }]) {
      expect(() => parseShutdownDeadline(invalid)).toThrow('Invalid shutdown deadline');
    }
  });
});

describe('worker acknowledgement and termination', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());
  const fixture = () => Object.assign(new EventEmitter(), { threadId: 1, postMessage: vi.fn(), terminate: vi.fn(async () => 1) });
  it('handles absent/exited workers and cleans listeners after a real acknowledgement', async () => {
    await stopRuntimeWorker(undefined);
    const worker = fixture(); worker.threadId = -1;
    await expect(stopRuntimeWorker(worker as unknown as Worker)).rejects.toThrow('exited before');
    worker.threadId = 1; const done = stopRuntimeWorker(worker as unknown as Worker);
    worker.emit('message', { type: 'unrelated' }); expect(worker.listenerCount('message')).toBe(1);
    worker.emit('message', { type: 'shutdown-complete' }); await done;
    expect(worker.listenerCount('message')).toBe(0); expect(worker.listenerCount('exit')).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });
  it.each(['timeout', 'send', 'terminate', 'early-exit', 'failed-ack', 'error'])('treats %s failure as incomplete, with handled promises', async failure => {
    const worker = fixture();
    if (failure === 'send') worker.postMessage.mockImplementation(() => { throw new Error('port closed'); });
    if (failure === 'terminate') worker.terminate.mockRejectedValue(new Error('failed'));
    const checked = expect(stopRuntimeWorker(worker as unknown as Worker)).rejects.toThrow();
    if (failure === 'early-exit') worker.emit('exit', 0);
    if (failure === 'failed-ack') worker.emit('message', { type: 'shutdown-failed' });
    if (failure === 'error') worker.emit('error', new Error('failed'));
    await vi.advanceTimersByTimeAsync(10000); await checked;
    expect(worker.listenerCount('message')).toBe(0); expect(vi.getTimerCount()).toBe(0);
  });
});
