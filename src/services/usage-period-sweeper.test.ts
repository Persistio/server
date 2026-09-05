import { describe, expect, it, vi } from 'vitest';

import { UsagePeriodSweeper } from './usage-period-sweeper';

describe('UsagePeriodSweeper', () => {
  it('does not overlap concurrent sweeps', async () => {
    let releaseSweep: (() => void) | undefined;
    const sweep = vi.fn(() => new Promise<{ closed: number; currentPeriod: string; selected: number }>((resolve) => {
      releaseSweep = () => resolve({ closed: 1, currentPeriod: '2026-07', selected: 1 });
    }));
    const sweeper = new UsagePeriodSweeper({
      batchSize: 25,
      intervalMs: 10000,
      sweep
    });

    const first = sweeper.sweepDuePeriods();
    await vi.waitFor(() => expect(sweep).toHaveBeenCalledOnce());
    await expect(sweeper.sweepDuePeriods()).resolves.toEqual({
      closed: 0,
      currentPeriod: '',
      selected: 0,
      skipped: true
    });

    releaseSweep?.();
    await expect(first).resolves.toEqual({
      closed: 1,
      currentPeriod: '2026-07',
      selected: 1,
      skipped: false
    });
  });

  it('waits for active sweep work before stopping', async () => {
    let releaseSweep: (() => void) | undefined;
    const sweep = vi.fn(() => new Promise<{ closed: number; currentPeriod: string; selected: number }>((resolve) => {
      releaseSweep = () => resolve({ closed: 1, currentPeriod: '2026-07', selected: 1 });
    }));
    const sweeper = new UsagePeriodSweeper({
      batchSize: 25,
      intervalMs: 10000,
      sweep
    });

    const run = sweeper.sweepDuePeriods();
    await vi.waitFor(() => expect(sweep).toHaveBeenCalledOnce());
    let stopped = false;
    const stop = sweeper.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();

    expect(stopped).toBe(false);

    releaseSweep?.();
    await run;
    await stop;

    expect(stopped).toBe(true);
  });
});
