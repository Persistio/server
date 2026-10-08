import crypto from 'node:crypto';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createRequestWorkerServer } from '../server';
import { requestWorkerConfig } from '../config';
import type { WorkerExecutionStore } from '../store';
import { superviseExecution, type ExecutionFactory } from '../supervisor';

const config = requestWorkerConfig({ REQUEST_WORKER_ENABLED: 'true', REQUEST_WORKER_AUDIENCE: 'https://worker-example.run.app',
  REQUEST_WORKER_INVOKER_EMAIL: 'tasks@example-project.iam.gserviceaccount.com', REQUEST_WORKER_INVOKER_SUBJECT: '123456789012345678901' });
const wake = { version: 1 as const, kind: 'extraction' as const, token: crypto.randomUUID(), generation: '1' };
const runtimes: ReturnType<typeof createRequestWorkerServer>[] = [];
function setup(code = "require('node:worker_threads').parentPort.postMessage({type:'settled',success:true}); setInterval(()=>{},1000)") {
  const store = { acquire: vi.fn().mockResolvedValue({ status: 'acquired', previousOwner: null }),
    renew: vi.fn().mockResolvedValue(true), clearBackends: vi.fn().mockResolvedValue(undefined), finish: vi.fn().mockResolvedValue(true) };
  const workers: Worker[] = [];
  const factory = vi.fn<ExecutionFactory>(() => { const worker = new Worker(code, { eval: true }); workers.push(worker); return worker; });
  const authenticate = vi.fn().mockResolvedValue(true); const fatal = vi.fn();
  const runtime = createRequestWorkerServer({ config: { ...config }, store: store as unknown as WorkerExecutionStore, authenticate, fatal, executionFactory: factory });
  runtimes.push(runtime);
  return { runtime, store, workers, factory, authenticate, fatal };
}
const deliver = (runtime: ReturnType<typeof createRequestWorkerServer>, payload: unknown = wake, headers = {}) => runtime.app.inject({ method: 'POST', url: '/internal/worker/wake', payload, headers });
afterEach(async () => { await Promise.all(runtimes.splice(0).map(runtime => runtime.stop())); });
describe('private request host', () => {
  it('authenticates but never admits SQL work when deployment admission is disabled', async () => {
    const store = { acquire: vi.fn() };
    const authenticate = vi.fn().mockResolvedValue(true);
    const factory = vi.fn();
    const runtime = createRequestWorkerServer({ config: { ...config, REQUEST_WORKER_ADMISSION_ENABLED: 'false' },
      store: store as unknown as WorkerExecutionStore, authenticate, fatal: vi.fn(), executionFactory: factory });
    runtimes.push(runtime);
    expect((await deliver(runtime)).statusCode).toBe(503);
    expect(authenticate).toHaveBeenCalledOnce();
    expect(store.acquire).not.toHaveBeenCalled();
    expect(factory).not.toHaveBeenCalled();
    authenticate.mockResolvedValue(false);
    expect((await deliver(runtime)).statusCode).toBe(401);
  });
  it('rejects authentication before SQL or thread startup even with forged task headers', async () => {
    const s = setup(); s.authenticate.mockResolvedValue(false);
    expect((await deliver(s.runtime, wake, { 'x-cloudtasks-taskname': 'forged', 'x-api-key': 'test-admin-key', 'x-serverless-authorization': 'Bearer unsigned' })).statusCode).toBe(401);
    expect(s.store.acquire).not.toHaveBeenCalled(); expect(s.factory).not.toHaveBeenCalled();
    expect((await s.runtime.app.inject('/v1/memories')).statusCode).toBe(404);
  });
  it('rejects malformed and oversized envelopes before admission', async () => {
    const s = setup();
    for (const payload of [{ ...wake, generation: 1 }, { ...wake, token: 'bad' }, { ...wake, extra: true }]) expect((await deliver(s.runtime, payload)).statusCode).toBe(400);
    expect((await deliver(s.runtime, { ...wake, extra: 'a'.repeat(2048) })).statusCode).toBe(413);
    expect(s.store.acquire).not.toHaveBeenCalled();
  });
  it('acknowledges stale identity without work; retries busy admission', async () => {
    const s = setup(); s.store.acquire.mockResolvedValueOnce({ status: 'stale' }).mockResolvedValueOnce({ status: 'busy' });
    expect((await deliver(s.runtime)).statusCode).toBe(204);
    expect((await deliver(s.runtime)).statusCode).toBe(503);
    expect(s.factory).not.toHaveBeenCalled(); expect(s.store.finish).not.toHaveBeenCalled();
  });
  it('only acknowledges after the successful thread has exited and SQL cleanup has completed', async () => {
    const s = setup(); const order: string[] = [];
    s.store.clearBackends.mockImplementation(async () => { expect(s.workers[0].threadId).toBe(-1); order.push('cleanup'); });
    s.store.finish.mockImplementation(async () => { order.push('ack'); return true; });
    expect((await deliver(s.runtime)).statusCode).toBe(204);
    expect(order).toEqual(['cleanup', 'ack']);
    expect(s.store.finish).toHaveBeenCalledWith(expect.any(String), wake, true);
  });
  it('does not acknowledge a crash, false result or silent exit', async () => {
    for (const code of ["throw new Error('crash')", "process.exit(0)", "require('node:worker_threads').parentPort.postMessage({type:'settled',success:false})"]) {
      const s = setup(code); s.store.finish.mockImplementation(async (_id, _wake, success) => success);
      expect((await deliver(s.runtime)).statusCode).toBe(503);
      expect(s.store.finish).toHaveBeenCalledWith(expect.any(String), wake, false);
    }
  });
  it('fails closed if backend cleanup or acknowledgement is uncertain', async () => {
    const s = setup(); s.store.clearBackends.mockRejectedValue(new Error('SQL unavailable'));
    expect((await deliver(s.runtime)).statusCode).toBe(503);
    expect(s.store.finish).not.toHaveBeenCalled(); expect(s.fatal).toHaveBeenCalledOnce();
    expect((await s.runtime.app.inject('/health')).statusCode).toBe(503);
    const t = setup(); t.store.finish.mockRejectedValue(new Error('lost COMMIT'));
    expect((await deliver(t.runtime)).statusCode).toBe(503); expect(t.fatal).toHaveBeenCalledOnce();
  });
  it('holds local admission during authentication', async () => {
    const s = setup(); let resolve!: (value: boolean) => void;
    s.authenticate.mockImplementationOnce(() => new Promise<boolean>(done => { resolve = done; }));
    const pending = deliver(s.runtime).then(response => response);
    await vi.waitFor(() => expect(s.authenticate).toHaveBeenCalledOnce());
    expect((await deliver(s.runtime)).statusCode).toBe(503);
    resolve(true); expect((await pending).statusCode).toBe(204);
  });
  it('terminates an uncooperative thread on shutdown and retains the wake for retry', async () => {
    const s = setup('while(true) {}'); s.store.finish.mockResolvedValue(false);
    const pending = deliver(s.runtime).then(response => response);
    await vi.waitFor(() => expect(s.factory).toHaveBeenCalledOnce());
    await s.runtime.stop();
    expect((await pending).statusCode).toBe(503);
    expect(s.workers[0].threadId).toBe(-1);
    expect(s.store.finish).toHaveBeenCalledWith(expect.any(String), wake, false);
  });
  it('cancels an execution when the real HTTP response connection disconnects', async () => {
    const s = setup('while(true) {}'); s.store.finish.mockResolvedValue(false);
    const base = await s.runtime.app.listen({ port: 0, host: '127.0.0.1' });
    const controller = new AbortController();
    const response = fetch(base + '/internal/worker/wake', { method: 'POST', signal: controller.signal,
      headers: { 'content-type': 'application/json' }, body: JSON.stringify(wake) }).catch(error => error);
    await vi.waitFor(() => expect(s.factory).toHaveBeenCalledOnce());
    controller.abort(); await response;
    await vi.waitFor(() => expect(s.store.finish).toHaveBeenCalledWith(expect.any(String), wake, false), { timeout: 4000 });
    expect(s.workers[0].threadId).toBe(-1);
  });
});
describe('execution supervisor', () => {
  it('hard-terminates an uncooperative worker at deadline without accepting its late work', async () => {
    let worker!: Worker;
    const start = Date.now();
    expect(await superviseExecution({ id: crypto.randomUUID(), wake, deadline: Date.now() + 100 }, { renew: vi.fn() }, new AbortController().signal,
      () => worker = new Worker('while(true) {}', { eval: true }))).toBe(false);
    expect(worker.threadId).toBe(-1); expect(Date.now() - start).toBeLessThan(5000);
  });
  it('cancels on lease loss and waits for exit', async () => {
    let worker!: Worker; const renew = vi.fn().mockResolvedValue(false);
    expect(await superviseExecution({ id: crypto.randomUUID(), wake, deadline: Date.now() + 30000 }, { renew }, new AbortController().signal,
      () => worker = new Worker("require('node:worker_threads').parentPort.on('message',m=>{if(m.type==='cancel')process.exit(0)})", { eval: true }))).toBe(false);
    expect(renew).toHaveBeenCalledOnce(); expect(worker.threadId).toBe(-1);
  }, 15000);
});
