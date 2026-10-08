import { getConfig } from '../config';

/** A failed aggregate still owns its sibling IO. Await settlement before the
 * guarded API sends a response; preserve the original error and legacy behavior. */
export async function requestAll<T extends readonly unknown[]>(values: T): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }> {
  try { return await Promise.all(values); }
  catch (error) {
    if (getConfig().API_REQUEST_BILLING) await Promise.allSettled(values);
    throw error;
  }
}
