import type { Pool } from 'pg';
import { afterEach, describe, expect, it, vi } from 'vitest';

afterEach(() => vi.resetModules());
async function initialize(saved: unknown = {}) {
  const context = await import('../context');
  context.installExecutionContext('fixture', new AbortController().signal, Date.now() + 60000);
  let snapshot: any;
  const query = vi.fn().mockImplementation(async (sql: string, args: unknown[]) => {
    if (sql.startsWith('SELECT')) return { rowCount: 1, rows: [{ runtime_state: saved }] };
    snapshot = JSON.parse(args[1] as string); return { rowCount: 1 };
  });
  const state = await import('../state'); await state.initializeExecutionState({ query } as unknown as Pool, 'fixture');
  return { context, state, query, snapshot: () => snapshot };
}
describe('execution state checkpoints', () => {
  it('persists a half-open probe as a timed open circuit, so a killed probe cannot mint another immediately', async () => {
    const s = await initialize({ 'circuit:fixture': { state: 'open', consecutiveFailures: 0, currentProbeIntervalMs: 300000,
      halfOpenProbeInFlight: false, nextProbeAt: Date.now() - 1000 } });
    const { ServiceCircuitBreaker } = await import('../../services/ai-resilience');
    const breaker = new ServiceCircuitBreaker('fixture'); breaker.beforeRequest();
    expect(breaker.getSnapshot().state).toBe('half-open');
    await s.state.checkpointExecutionState();
    const saved = s.snapshot(); expect(saved['circuit:fixture']).toMatchObject({ state: 'open', halfOpenProbeInFlight: false });
    expect(saved['circuit:fixture'].nextProbeAt).toBeGreaterThan(Date.now());
    vi.resetModules(); await initialize(saved);
    const NextBreaker = (await import('../../services/ai-resilience')).ServiceCircuitBreaker;
    expect(() => new NextBreaker('fixture').beforeRequest()).toThrow('circuit breaker is open');
  });
  it('invalidates the owner on an unknown checkpoint result and prevents further business work', async () => {
    const s = await initialize(); s.state.registerExecutionState('fixture', () => {}, () => ({ charge: 1 }));
    s.query.mockRejectedValueOnce(new Error('lost COMMIT response'));
    await expect(s.state.checkpointExecutionState()).rejects.toThrow('lost COMMIT');
    expect(() => s.context.assertExecutionLive()).toThrow('invalidated');
    await expect(s.state.checkpointExecutionState()).rejects.toThrow('invalidated');
    expect(s.query).toHaveBeenCalledTimes(2);
  });
  it('refuses malformed persisted circuit state before a provider can be called', async () => {
    await initialize({ 'circuit:fixture': { state: 'open', currentProbeIntervalMs: 'invalid' } });
    const { ServiceCircuitBreaker } = await import('../../services/ai-resilience');
    expect(() => new ServiceCircuitBreaker('fixture')).toThrow();
  });
});
