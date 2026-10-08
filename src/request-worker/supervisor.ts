import path from 'node:path';
import { Worker } from 'node:worker_threads';
import type { WorkerWake } from '../services/worker-wake-types';
import type { WorkerExecutionStore } from './store';

export interface ExecutionInvocation { id: string; wake: WorkerWake; deadline: number }
export type ExecutionFactory = (invocation: ExecutionInvocation) => Worker;

export const createExecutionThread: ExecutionFactory = invocation => new Worker(
  path.resolve(__dirname, 'execution-thread.js'), {
    execArgv: [], workerData: invocation,
    // request-worker mode is explicit; do not inherit a telemetry preload.
    env: { ...process.env, PERSISTIO_MODE: 'request-worker', TELEMETRY_PROVIDER: 'none',
      CUSTOMER_METRICS_PUBLISHER: 'noop', DB_POOL_MAX: '2' }
  }
);

/** Owns every child/timer/renewal until exit. Result messages cannot finish HTTP work. */
export async function superviseExecution(
  invocation: ExecutionInvocation,
  store: Pick<WorkerExecutionStore, 'renew'>,
  signal: AbortSignal,
  factory: ExecutionFactory = createExecutionThread
): Promise<boolean> {
  if (signal.aborted) return false;
  const worker = factory(invocation);
  let success = false;
  let cancelled = false;
  let renewal: Promise<void> | undefined;
  let termination: Promise<number> | undefined;
  let hardStop: NodeJS.Timeout | undefined;
  const terminate = () => { termination ??= worker.terminate(); void termination.catch(() => {}); };
  const cancel = () => {
    if (cancelled) return;
    cancelled = true;
    worker.postMessage({ type: 'cancel' });
    hardStop = setTimeout(terminate, 2000);
  };
  const expired = setTimeout(cancel, Math.max(0, invocation.deadline - Date.now()));
  const heartbeat = setInterval(() => {
    if (renewal || cancelled || termination) return;
    renewal = (async () => {
      try {
        if (!await store.renew(invocation.id)) cancel();
        else if (!cancelled && !termination) worker.postMessage({ type: 'heartbeat' });
      } catch { cancel(); }
    })().finally(() => { renewal = undefined; });
  }, 10_000);
  signal.addEventListener('abort', cancel, { once: true });
  if (signal.aborted) cancel();
  try {
    await new Promise<void>(resolve => {
      worker.on('message', message => {
        if (message?.type !== 'settled' || termination) return;
        success = message.success === true && !cancelled && Date.now() < invocation.deadline;
        terminate();
      });
      worker.on('error', () => { success = false; terminate(); });
      worker.once('exit', () => resolve());
    });
    // stop producing renewals before awaiting any already admitted control query
    clearInterval(heartbeat);
    await renewal;
    await termination;
    return success && !cancelled && !signal.aborted;
  } finally {
    clearInterval(heartbeat);
    clearTimeout(expired);
    if (hardStop) clearTimeout(hardStop);
    signal.removeEventListener('abort', cancel);
  }
}
