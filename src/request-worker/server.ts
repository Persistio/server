import { shadowWorkerCycle } from '../services/shadow-observability';
import crypto from 'node:crypto';
import Fastify from 'fastify';
import { workerWakeSchema, type WorkerWakeKind } from '../services/worker-wake-types';
import type { RequestWorkerConfig } from './config';
import type { WorkerExecutionStore } from './store';
import { superviseExecution, type ExecutionFactory } from './supervisor';

export function createRequestWorkerServer(options: {
  config: RequestWorkerConfig;
  store: WorkerExecutionStore;
  authenticate: (authorization: string | undefined) => Promise<boolean>;
  fatal: () => void;
  executionFactory?: ExecutionFactory;
  continueWork?: (kind: WorkerWakeKind) => Promise<unknown>;
}) {
  const app = Fastify({ logger: false, bodyLimit: 1024, requestTimeout: 10_000 });
  let busy = false;
  let stopping = false;
  let active: AbortController | undefined;
  app.get('/health', async (_request, reply) => stopping ? reply.code(503).send() : { status: 'ok' });
  app.post('/internal/worker/wake', async (request, reply) => {
    if (stopping || busy) return reply.code(503).send();
    busy = true;
    const abort = new AbortController();
    active = abort;
    // One wall-clock budget includes auth, admission and expired-owner cleanup.
    const deadline = Date.now() + options.config.REQUEST_WORKER_DEADLINE_MS;
    const requestDeadline = setTimeout(() => abort.abort(), options.config.REQUEST_WORKER_DEADLINE_MS);
    const disconnect = () => { if (!reply.raw.writableFinished) abort.abort(); };
    reply.raw.once('close', disconnect);
    const started = performance.now();
    let outcome: 'completed' | 'failed' | 'stale' | 'busy' = 'failed';
    let owner: string | undefined;
    let wake: ReturnType<typeof workerWakeSchema.parse> | undefined;
    let cleanupDeadline: NodeJS.Timeout | undefined;
    const armFatal = () => {
      cleanupDeadline ??= setTimeout(() => { stopping = true; options.fatal(); }, 8000);
    };
    abort.signal.addEventListener('abort', armFatal, { once: true });
    try {
      if (!await options.authenticate(request.headers.authorization)) return reply.code(401).send();
      if (abort.signal.aborted || stopping || (options.config.REQUEST_WORKER_ADMISSION_ENABLED === 'false' && options.config.REQUEST_WORKER_CUTOVER_ENABLED !== 'true')) {
        return reply.code(503).send();
      }
      const parsed = workerWakeSchema.safeParse(request.body);
      if (!parsed.success) return reply.code(400).send();
      wake = parsed.data;
      const candidate = crypto.randomUUID();
      const admission = await options.store.acquire(candidate, wake);
      if (admission.status === 'busy') { outcome='busy'; return reply.code(503).send(); }
      if (admission.status === 'stale') { outcome='stale'; return reply.code(204).send(); }
      owner = candidate;
      if (admission.previousOwner) await options.store.clearBackends(admission.previousOwner);
      const success = await superviseExecution({ id: owner, wake, deadline }, options.store, abort.signal, options.executionFactory);
      // Also bound cleanup after natural completion, not just cancellation.
      armFatal();
      await options.store.clearBackends(owner);
      const acknowledged = await options.store.finish(owner, wake, success && !abort.signal.aborted);
      outcome = acknowledged ? 'completed' : 'failed';
      owner = undefined;
      // Business is already durable and SQL is settled. Continuation owns no
      // business retry and remains awaited inside the original request deadline.
      if (cleanupDeadline) { clearTimeout(cleanupDeadline); cleanupDeadline = undefined; }
      if (acknowledged && !abort.signal.aborted && !stopping && options.continueWork) {
        try { await options.continueWork(wake.kind); }
        catch { /* Minute reconciliation repairs missed continuation. */ }
      }
      return reply.code(acknowledged ? 204 : 503).send();
    } catch {
      // Unknown admission/COMMIT/cleanup leaves the lease for fenced recovery.
      // Never retry business work here or claim successful completion.
      if (owner) { stopping = true; options.fatal(); }
      return reply.code(503).send();
    } finally {
      if (wake) shadowWorkerCycle(wake.kind, outcome, performance.now()-started);
      clearTimeout(requestDeadline);
      if (cleanupDeadline) clearTimeout(cleanupDeadline);
      reply.raw.removeListener('close', disconnect);
      active = undefined;
      busy = false;
    }
  });
  return {
    app,
    async stop() {
      stopping = true;
      active?.abort();
      await app.close();
    }
  };
}
