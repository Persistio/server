import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PoolClient } from 'pg';

afterEach(() => { vi.useRealTimers(); vi.resetModules(); vi.unstubAllGlobals(); });
describe('request execution context', () => {
  it('leaves legacy calls and pool configuration unchanged', async () => {
    const context = await import('../context');
    expect(context.executionSignal()).toBeUndefined();
    expect(context.executionDatabaseOptions()).toEqual({});
    expect(context.executionApplicationName('legacy')).toBe('legacy');
    const query = vi.fn(); const client = { query } as unknown as PoolClient;
    context.guardExecutionClient(client);
    expect(client.query).toBe(query);
  });
  it('rejects catch-path updates and commit after cancellation but allows rollback', async () => {
    const context = await import('../context');
    const controller = new AbortController();
    context.installExecutionContext('fixture', controller.signal, Date.now() + 60000);
    const query = vi.fn(); const client = { query } as unknown as PoolClient;
    context.guardExecutionClient(client);
    client.query('BEGIN');
    controller.abort(new Error('deadline'));
    for (const sql of ['COMMIT', 'UPDATE extraction_queue SET retry_count=retry_count+1']) expect(() => client.query(sql)).toThrow('deadline');
    client.query('ROLLBACK');
    expect(query.mock.calls.map(call => call[0])).toEqual(['BEGIN', 'ROLLBACK']);
    expect(context.executionDatabaseOptions()).toMatchObject({ max: 2, statement_timeout: 5000, lock_timeout: 1000 });
  });
  it('cannot revive a stalled owner with a late heartbeat', async () => {
    vi.useFakeTimers({ toFake: ['performance', 'Date'] });
    const context = await import('../context');
    context.installExecutionContext('fixture', new AbortController().signal, Date.now() + 120000);
    vi.advanceTimersByTime(30001);
    expect(() => context.executionHeartbeat()).toThrow('ownership expired');
    expect(() => context.assertExecutionLive()).toThrow('ownership expired');
  });
  it('interrupts provider retry backoff and combines transport cancellation', async () => {
    const context = await import('../context');
    const controller = new AbortController();
    context.installExecutionContext('fixture', controller.signal, Date.now() + 60000);
    const backoff = context.executionDelay(60000);
    const fetcher = vi.fn().mockResolvedValue(new Response()); vi.stubGlobal('fetch', fetcher);
    await context.executionFetch('https://example.test', { signal: new AbortController().signal });
    controller.abort(new Error('cancelled'));
    await expect(backoff).rejects.toThrow('cancelled');
    expect(fetcher.mock.calls[0][1].signal.aborted).toBe(true);
  });
});
