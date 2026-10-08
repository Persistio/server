import Fastify from 'fastify';

export class ScheduledCleanupError extends Error {}

/** Explicit scheduled requests only. Unknown I/O is terminated with the process,
 * never acknowledged while background SQL/publication can still be running. */
export function createScheduledWorkerServer(options: {
  authenticate: (authorization: string | undefined) => Promise<boolean>;
  observe: () => Promise<0 | 1>;
  reconcile: () => Promise<void>;
  fatal: () => void;
  deadlineMs?: number;
}) {
  const app = Fastify({ logger: false, bodyLimit: 1024, requestTimeout: 10_000 });
  let busy = false;
  let stopping = false;
  app.get('/health', async (_request, reply) => stopping ? reply.code(503).send() : { status: 'ok' });
  for (const operation of ['observe', 'reconcile'] as const) {
    app.post(`/internal/${operation}`, async (request, reply) => {
      if (stopping || busy) return reply.code(503).send();
      busy = true;
      const terminate = () => { stopping = true; options.fatal(); };
      const timer = setTimeout(terminate, options.deadlineMs ?? 55_000);
      const disconnect = () => { if (!reply.raw.writableFinished) terminate(); };
      reply.raw.once('close', disconnect);
      try {
        if (!await options.authenticate(request.headers.authorization)) return reply.code(401).send();
        if (stopping) return reply.code(503).send();
        // Schedules have no input contract. Never accept target URLs, SQL or topics.
        if (request.body !== undefined && request.body !== null && request.body !== '') return reply.code(400).send();
        const status = operation === 'observe' ? await options.observe() : (await options.reconcile(), 0);
        return reply.code(stopping || status !== 0 ? 503 : 204).send();
      } catch (error) {
        if (error instanceof ScheduledCleanupError) terminate();
        return reply.code(503).send();
      } finally {
        clearTimeout(timer);
        reply.raw.removeListener('close', disconnect);
        busy = false;
      }
    });
  }
  return {
    app,
    async stop() { stopping = true; await app.close(); }
  };
}
