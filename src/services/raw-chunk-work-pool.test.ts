import { describe, expect, it } from 'vitest';
import { RawChunkWorkPool } from './raw-chunk-work-pool';

describe('raw chunk actual-work bounds', () => {
  it('retains all four slots after deadline, cancels queued work, then recovers on actual settlement', async () => {
    const pool = new RawChunkWorkPool();
    const resolve: Array<() => void> = [];
    const started: number[] = [];
    await expect(pool.map(Array.from({ length: 2048 }, (_, i) => i), async i => {
      started.push(i);
      await new Promise<void>(done => resolve.push(done));
      return i;
    }, 5)).rejects.toThrow('deadline');
    expect(started).toEqual([0, 1, 2, 3]);
    await expect(pool.map([99], async i => i)).rejects.toThrow('capacity');
    resolve.forEach(done => done());
    await new Promise(done => setImmediate(done));
    expect(started).toEqual([0, 1, 2, 3]);
    expect(await pool.map([10, 11], async i => i)).toEqual([10, 11]);
  });
  it('bounds overlapping requests, preserves ordering and stops queued work after a failure', async () => {
    const pool = new RawChunkWorkPool(2);
    let release!: () => void;
    const first = pool.map([1], () => new Promise<number>(done => { release = () => done(1); }));
    let active = 1;
    let maximum = active;
    const result = await pool.map([2, 3, 4], async n => {
      maximum = Math.max(maximum, ++active);
      await new Promise(done => setImmediate(done));
      active--;
      return n;
    });
    expect(maximum).toBe(2);
    expect(result).toEqual([2, 3, 4]);
    release();
    await first;
    let started = 0;
    await expect(pool.map([0, 1, 2, 3], async () => { started++; throw new Error('provider failed'); })).rejects.toThrow('provider failed');
    expect(started).toBe(2);
    expect(await pool.map([5], async n => n)).toEqual([5]);
  });
});
