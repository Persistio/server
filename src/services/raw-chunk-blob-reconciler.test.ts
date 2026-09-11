import { describe, expect, it, vi } from 'vitest';

import { RawChunkBlobReconciler, reconcileStaleRawChunkBlobWrites } from './raw-chunk-blob-reconciler';

function transactionWith(rows: Array<Record<string, unknown>>) {
  const query = vi.fn(async (sql: string, params: unknown[] = []) => {
    if (sql.startsWith('SELECT id FROM raw_chunk_blob_write_intents')) return { rowCount: rows.length, rows };
    if (sql.startsWith('SELECT id, blob_store')) {
      const found = rows.filter(row => row.id === params[0]).map(row => ({ ...row, write_phase: 'uploaded' }));
      return { rowCount: found.length, rows: found };
    }
    if (sql.startsWith('SELECT id FROM raw_chunks')) {
      const found = rows.filter(row => row.blob_store === params[0] && row.blob_key === params[1] && row.raw_chunk_id);
      return { rowCount: found.length, rows: found };
    }
    return { rowCount: 1, rows: [] };
  });
  return {
    query,
    transaction: async <T>(callback: (client: { query: typeof query }) => Promise<T>) => callback({ query } as never)
  };
}

describe('raw chunk blob reconciliation', () => {
  it('deletes only proven orphans and clears intents for committed references', async () => {
    const committed = { id: 'intent-1', blob_store: 'local', blob_key: 'committed.txt', raw_chunk_id: 'chunk-1' };
    const orphan = { id: 'intent-2', blob_store: 'local', blob_key: 'orphan.txt', raw_chunk_id: null };
    const db = transactionWith([committed, orphan]);
    const storage = { store: 'local' as const, put: vi.fn(), get: vi.fn(), delete: vi.fn().mockResolvedValue(undefined) };

    const result = await reconcileStaleRawChunkBlobWrites(10, 1_000, storage, db.transaction as never);

    expect(result).toEqual({ selected: 2, committed: 1, deleted: 1, failed: 0, quarantined: 0 });
    expect(storage.delete).toHaveBeenCalledExactlyOnceWith('orphan.txt');
    expect(db.query.mock.calls.filter(([sql]) => String(sql).startsWith('DELETE FROM'))).toHaveLength(2);
  });

  it('retains mismatched-store and failed-delete intents for a later safe retry', async () => {
    const db = transactionWith([
      { id: 'intent-1', blob_store: 'gcs', blob_key: 'remote.txt', raw_chunk_id: null },
      { id: 'intent-2', blob_store: 'local', blob_key: 'failed.txt', raw_chunk_id: null }
    ]);
    const storage = {
      store: 'local' as const,
      put: vi.fn(),
      get: vi.fn(),
      delete: vi.fn().mockRejectedValue(new Error('storage unavailable'))
    };

    const result = await reconcileStaleRawChunkBlobWrites(10, 1_000, storage, db.transaction as never);

    expect(result).toEqual({ selected: 2, committed: 0, deleted: 0, failed: 2, quarantined: 0 });
    expect(storage.delete).toHaveBeenCalledExactlyOnceWith('failed.txt');
    expect(db.query.mock.calls.filter(([sql]) => String(sql).includes('SET last_error'))).toHaveLength(2);
  });

  it('coalesces overlapping scheduled sweeps', async () => {
    let finish!: (value: { selected: number; committed: number; deleted: number; failed: number }) => void;
    const reconcile = vi.fn(() => new Promise<typeof zero>((resolve) => { finish = resolve; }));
    const zero = { selected: 0, committed: 0, deleted: 0, failed: 0 };
    const worker = new RawChunkBlobReconciler({ batchSize: 10, intervalMs: 60_000, reconcile });

    const first = worker.run();
    await Promise.resolve();
    expect(await worker.run()).toEqual({ ...zero, skipped: true });
    finish(zero);
    expect(await first).toEqual({ ...zero, skipped: false });
    expect(reconcile).toHaveBeenCalledTimes(1);
  });
});
