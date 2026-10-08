import { getConfig } from '../config';

/** Per-process backpressure. A caller deadline never frees an actual-work slot. */
export class RawChunkWorkPool {
  private active = 0;
  constructor(private readonly capacity = 4,
    private readonly settlement = {
      required: () => getConfig().API_REQUEST_BILLING
    }) {}

  async map<T, R>(items: readonly T[], work: (item: T, index: number, signal?: AbortSignal) => Promise<R>, timeoutMs = 30_000): Promise<R[]> {
    if (!items.length) return [];
    if (this.active >= this.capacity) throw Object.assign(new Error('Raw chunk provider capacity exhausted'), { statusCode: 503 });
    const abort = this.settlement.required() ? new AbortController() : undefined;
    const results = new Array<R>(items.length);
    let next = 0;
    let cancelled = false;
    const workers: Promise<void>[] = [];
    // Reserve before invoking asynchronous work: competing requests cannot acquire
    // these slots, and each worker retains its slot across its actual provider IO.
    const count = Math.min(items.length, this.capacity - this.active);
    this.active += count;
    for (let worker = 0; worker < count; worker++) {
      workers.push((async () => {
        try {
          while (!cancelled && next < items.length) {
            const index = next++;
            results[index] = await work(items[index], index, abort?.signal);
          }
        } catch (error) { cancelled = true; abort?.abort(error); throw error; }
        finally { this.active--; }
      })());
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.all(workers),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => {
            cancelled = true;
            abort?.abort(new Error('Raw chunk deadline exceeded'));
            reject(Object.assign(new Error('Raw chunk provider deadline exceeded'), { statusCode: 503 }));
          }, timeoutMs);
        })
      ]);
      return results;
    } finally {
      if (timer) clearTimeout(timer);
      cancelled = true;
      if (this.settlement.required()) {
        // Promise.all may reject before sibling PUT/embedding/SQL settles. Never
        // return a response with those continuations still owning business IO.
        await Promise.allSettled(workers);
      }
    }
  }
}

export const rawChunkPreparationPool = new RawChunkWorkPool();
export const rawChunkUploadPool = new RawChunkWorkPool();
