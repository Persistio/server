import type { Worker } from 'node:worker_threads';

/** Absolute process-monotonic times, shared unchanged with worker threads. */
export interface ShutdownDeadline { io: bigint; process: bigint }
const MS = 1_000_000n;
export function createShutdownDeadline(cloudRun: boolean): ShutdownDeadline | undefined {
  const now = process.hrtime.bigint();
  return cloudRun ? { io: now + 8000n * MS, process: now + 9000n * MS } : undefined;
}

export function parseShutdownDeadline(value: unknown): ShutdownDeadline | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'object' || value === null) throw new Error('Invalid shutdown deadline');
  const { io, process: end } = value as Partial<ShutdownDeadline>;
  if (typeof io !== 'bigint' || typeof end !== 'bigint' || io <= 0n || end - io !== 1000n * MS ||
      end > process.hrtime.bigint() + 9000n * MS) throw new Error('Invalid shutdown deadline');
  return { io, process: end };
}

type Outcome = 'complete' | 'failed' | 'timeout';
const invoke = (fn: () => Promise<unknown> | undefined) => Promise.resolve().then(fn);

/** An admitted response can become idle AFTER server.close's initial sweep. */
export async function closeRuntimeHttp(close: () => Promise<void>, closeIdle: () => void): Promise<void> {
  let sweepFailed = false;
  const sweep = setInterval(() => {
    try { closeIdle(); } catch { sweepFailed = true; }
  }, 100);
  sweep.unref();
  try { await close(); }
  finally { clearInterval(sweep); }
  if (sweepFailed) throw new Error('HTTP idle cleanup failed');
}

/** Observes late rejection; a timeout never cancels the underlying operation. */
export function waitForShutdown(operation: Promise<unknown>, cutoff?: bigint): Promise<Outcome> {
  return new Promise(resolve => {
    let timer: NodeJS.Timeout | undefined;
    let settled = false;
    const finish = (outcome: Outcome) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      resolve(outcome);
    };
    void operation.then(() => finish('complete'), () => finish('failed'));
    if (cutoff !== undefined) {
      const remaining = Number(cutoff - process.hrtime.bigint()) / Number(MS);
      if (remaining <= 0) finish('timeout');
      else timer = setTimeout(() => finish('timeout'), remaining);
    }
  });
}

export async function drainRuntimeOwner(options: {
  drain: Array<() => Promise<unknown> | undefined>;
  publishers: Array<() => Promise<unknown> | undefined>;
  telemetry: () => Promise<void>;
  pool: () => Promise<void> | undefined;
  deadline?: ShutdownDeadline;
}): Promise<void> {
  const drained = await Promise.allSettled(options.drain.map(invoke));
  if (drained.some(result => result.status === 'rejected')) throw new Error('Runtime work drain failed');
  const deadline = options.deadline;
  if (deadline && process.hrtime.bigint() >= deadline.io) throw new Error('Runtime cleanup deadline reached');
  const publishers = options.publishers.map(close => waitForShutdown(invoke(close), deadline?.io));
  // On Cloud Run, these publishers do not produce memory metrics after work
  // drain. Start final collection independently so publisher IO cannot starve it.
  // Other hosts retain publisher-before-SDK ordering and no new export timeout.
  const publisherResults = deadline ? undefined : await Promise.all(publishers);
  const telemetry = await waitForShutdown(invoke(options.telemetry), deadline?.io);
  // A timed-out or rejected SDK shutdown is not proof its callbacks stopped.
  // Do not close its pool or report clean completion on that evidence.
  const pool = telemetry === 'complete'
    ? await waitForShutdown(invoke(options.pool), deadline?.process) : 'failed';
  const results = publisherResults ?? await Promise.all(publishers);
  if (telemetry !== 'complete' || pool !== 'complete' || results.some(r => r !== 'complete')) {
    throw new Error('Runtime cleanup incomplete');
  }
}

export function createRuntimeShutdown(options: {
  branches: Array<(deadline?: ShutdownDeadline) => Promise<void>>;
  cloudRun: boolean;
  warn: (message: string) => void;
  exit: (code: number) => void;
}): () => Promise<void> {
  let pending: Promise<void> | undefined;
  return () => pending ??= (async () => {
    const deadline = createShutdownDeadline(options.cloudRun);
    const results = options.branches.map(branch =>
      waitForShutdown(invoke(() => branch(deadline)), deadline?.process));
    const failed = (await Promise.all(results)).some(result => result !== 'complete');
    if (failed) {
      try { options.warn('Shutdown incomplete; work or cleanup did not finish'); }
      catch { /* Diagnostics cannot block termination. */ }
    }
    options.exit(failed ? 1 : 0);
  })();
}

export function stopRuntimeWorker(target: Worker | undefined, deadline?: ShutdownDeadline): Promise<void> {
  if (!target) return Promise.resolve();
  if (target.threadId === -1) return Promise.reject(new Error('Worker exited before shutdown'));
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let terminating = false;
    let timer: NodeJS.Timeout | undefined;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      target.off('message', onMessage);
      target.off('exit', onExit);
      target.off('error', onError);
      error ? reject(error) : resolve();
    };
    const terminate = () => {
      if (settled || terminating) return;
      terminating = true;
      void invoke(() => target.terminate()).then(
        () => finish(new Error('Worker required termination')),
        () => finish(new Error('Worker termination failed'))
      );
    };
    const onMessage = (message: unknown) => {
      if (terminating || typeof message !== 'object' || message === null) return;
      const type = (message as { type?: unknown }).type;
      if (type === 'shutdown-complete') finish();
      if (type === 'shutdown-failed') finish(new Error('Worker shutdown failed'));
    };
    const onExit = () => finish(new Error('Worker exited without shutdown acknowledgement'));
    const onError = () => finish(new Error('Worker failed during shutdown'));
    // The parent owns the Cloud Run cutoff. Preserve the existing fallback on
    // other hosts; do not give delayed Cloud Run children another ten seconds.
    if (!deadline) timer = setTimeout(terminate, 10000);
    target.on('message', onMessage);
    target.once('exit', onExit);
    target.once('error', onError);
    try { target.postMessage({ type: 'shutdown', deadline }); } catch { terminate(); }
  });
}
