import type { Client, PoolClient } from 'pg';

/** Installed only in a disposable request execution thread, before business imports. */
interface ExecutionContext {
  id: string;
  signal: AbortSignal;
  deadline: number;
  lastHeartbeat: number;
}
let context: ExecutionContext | undefined;
let invalid = false;
export function invalidateExecution(): void { invalid = true; }

export function installExecutionContext(id: string, signal: AbortSignal, deadline: number): void {
  if (context) throw new Error('Execution context already installed');
  context = { id, signal, deadline, lastHeartbeat: performance.now() };
}
export function executionHeartbeat(): void {
  assertExecutionLive();
  if (context) context.lastHeartbeat = performance.now();
}
export function assertExecutionLive(): void {
  if (!context) return;
  if (invalid) throw new Error('Request execution invalidated');
  context.signal.throwIfAborted();
  if (Date.now() >= context.deadline || performance.now() - context.lastHeartbeat >= 30_000) {
    throw new Error('Request execution ownership expired');
  }
}
export function executionSignal(): AbortSignal | undefined {
  assertExecutionLive();
  return context?.signal;
}
export function executionApplicationName(fallback: string): string {
  return context ? 'persistio:req:' + context.id : fallback;
}
export function executionDatabaseOptions() {
  return context ? {
    max: 2, connectionTimeoutMillis: 3000, statement_timeout: 5000,
    lock_timeout: 1000, idle_in_transaction_session_timeout: 5000
  } : {};
}
export async function executionDelay(ms: number): Promise<void> {
  const signal = executionSignal();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(); }, ms);
    const abort = () => { clearTimeout(timer); reject(signal?.reason); };
    signal?.addEventListener('abort', abort, { once: true });
  });
}
export function executionFetch(input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) {
  const signal = executionSignal();
  return fetch(input, signal ? { ...init, signal: init?.signal ? AbortSignal.any([signal, init.signal]) : signal } : init);
}

const guarded = new WeakSet<Client | PoolClient>();
/** Existing pooled clients and dedicated maintenance sessions share the same
 * liveness guard. Rollback remains possible after cancellation; COMMIT does not. */
export function guardExecutionClient(client: Client | PoolClient): void {
  if (!context || guarded.has(client)) return;
  const query = client.query;
  client.query = ((...args: unknown[]) => {
    const first = args[0];
    const sql = typeof first === 'string' ? first : (first as { text?: string })?.text;
    if (sql?.trim().toUpperCase() !== 'ROLLBACK') assertExecutionLive();
    return Reflect.apply(query, client, args);
  }) as typeof client.query;
  guarded.add(client);
}
