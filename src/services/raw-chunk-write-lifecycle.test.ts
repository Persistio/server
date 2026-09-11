import { describe, expect, it, vi } from 'vitest';

import { cleanupRawChunkWrite, MAX_IN_FLIGHT_RAW_CHUNK_DELETES, type WritePhase } from './raw-chunk-write-lifecycle';

function harness(phase: WritePhase = 'uploading') {
  const state = { exists: true, revoked: false, phase, referenced: false, committedTransactions: 0 };
  const statements: string[] = [];
  const query = vi.fn(async (sql: string) => {
    statements.push(sql);
    if (sql.startsWith('SELECT id, blob_store')) return {
      rowCount: state.exists ? 1 : 0,
      rows: state.exists ? [{ id: 'intent', blob_store: 'local', blob_key: 'blob', write_phase: state.phase }] : []
    };
    if (sql.startsWith('SELECT id FROM raw_chunks')) return { rowCount: state.referenced ? 1 : 0, rows: [] };
    if (sql.startsWith('DELETE FROM')) state.exists = false;
    if (sql.includes('SET revoked = true')) state.revoked = true;
    return { rowCount: 1, rows: [] };
  });
  const transaction = async <T>(run: (client: { query: typeof query }) => Promise<T>) => {
    const result = await run({ query });
    state.committedTransactions++;
    return result;
  };
  const storage = { store: 'local' as const, put: vi.fn(), get: vi.fn(), delete: vi.fn(async () => {
    expect(state.revoked).toBe(true);
    expect(state.committedTransactions).toBeGreaterThan(0);
  }) };
  return { state, statements, query, transaction, storage };
}

describe('raw upload cleanup ownership', () => {
  it('commits revocation before deleting and retires a confirmed completed upload', async () => {
    const h = harness('uploaded');
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('deleted');
    expect(h.state.exists).toBe(false);
    expect(h.statements[0]).toContain('FOR UPDATE SKIP LOCKED');
    expect(h.statements[1]).toContain('SELECT id FROM raw_chunks');
  });

  it('keeps the blob and clears only the marker for a committed raw reference', async () => {
    const h = harness();
    h.state.referenced = true;
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('committed');
    expect(h.storage.delete).not.toHaveBeenCalled();
    expect(h.state.exists).toBe(false);
  });

  it('retains an uncertain marker so a later PUT is still tracked', async () => {
    const h = harness();
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('quarantined');
    expect(h.state.exists).toBe(true);
    h.state.phase = 'uploaded';
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('deleted');
    expect(h.storage.delete).toHaveBeenCalledTimes(2);
  });

  it('does not retire when PUT completion is reported between DELETE and settlement', async () => {
    const h = harness();
    h.storage.delete.mockImplementationOnce(async () => { h.state.phase = 'uploaded'; });
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('quarantined');
    expect(h.state.exists).toBe(true);
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('deleted');
    expect(h.storage.delete).toHaveBeenCalledTimes(2);
  });

  it('does not invoke deletion on a failed proof transaction', async () => {
    const h = harness('uploaded');
    h.query.mockRejectedValueOnce(new Error('SQL unavailable'));
    await expect(cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).rejects.toThrow('SQL unavailable');
    expect(h.storage.delete).not.toHaveBeenCalled();
  });

  it('retains a failed-delete marker and schedules a later attempt', async () => {
    const h = harness('uploaded');
    h.storage.delete.mockRejectedValue(new Error('private provider detail'));
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('failed');
    expect(h.state.exists).toBe(true);
    expect(h.statements.at(-1)).toContain('updated_at = now()');
  });

  it('bounds a hung deletion without dropping its durable fence', async () => {
    const h = harness('uploaded');
    let finish!: () => void;
    h.storage.delete.mockImplementation(() => new Promise<void>(resolve => { finish = resolve; }));
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never, 5)).toBe('failed');
    expect(h.state.exists).toBe(true);
    expect(h.state.revoked).toBe(true);
    finish();
  });

  it('does not call the wrong object provider', async () => {
    const h = harness();
    const storage = { ...h.storage, store: 'gcs' as const };
    expect(await cleanupRawChunkWrite(storage, { id: 'intent' }, h.transaction as never)).toBe('failed');
    expect(h.storage.delete).not.toHaveBeenCalled();
    expect(h.state.exists).toBe(true);
  });

  it('keeps timed-out IO in the process-wide bound and never reuses an earlier DELETE as completion proof', async () => {
    const finish: Array<() => void> = [];
    const attempts = Array.from({ length: MAX_IN_FLIGHT_RAW_CHUNK_DELETES + 1 }, (_unused, index) => {
      const h = harness('uploaded');
      h.query.mockImplementation(async (sql: string) => {
        if (sql.startsWith('SELECT id, blob_store')) return { rowCount: 1,
          rows: [{ id: `intent-${index}`, blob_store: 'local', blob_key: `bounded-${index}`, write_phase: 'uploaded' }] };
        return { rowCount: 0, rows: [] };
      });
      h.storage.delete.mockImplementation(() => new Promise<void>(resolve => { finish.push(resolve); }));
      return h;
    });
    try {
      const results = await Promise.all(attempts.map(h => cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never, 5)));
      expect(results).toEqual(attempts.map(() => 'failed'));
      expect(finish).toHaveLength(MAX_IN_FLIGHT_RAW_CHUNK_DELETES);
      // Repeating a timed-out key must not attach to its earlier DELETE. That
      // earlier operation may have deleted before a subsequently confirmed PUT.
      const repeated = attempts[0];
      expect(await cleanupRawChunkWrite(repeated.storage, { id: 'intent' }, repeated.transaction as never, 5)).toBe('failed');
      expect(repeated.storage.delete).toHaveBeenCalledTimes(1);
      expect(attempts.at(-1)!.storage.delete).not.toHaveBeenCalled();
    } finally {
      finish.forEach(resolve => resolve());
      await new Promise(resolve => setImmediate(resolve));
    }
    const last = attempts.at(-1)!;
    last.storage.delete.mockResolvedValue(undefined);
    expect(await cleanupRawChunkWrite(last.storage, { id: 'intent' }, last.transaction as never, 5)).toBe('deleted');
  });

  it('does no work for an already consumed or concurrently locked intent', async () => {
    const h = harness();
    h.state.exists = false;
    expect(await cleanupRawChunkWrite(h.storage, { id: 'intent' }, h.transaction as never)).toBe('busy');
    expect(h.storage.delete).not.toHaveBeenCalled();
  });
});
