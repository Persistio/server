import { describe, expect, it, vi } from 'vitest';
import type { PoolClient } from 'pg';

import { enqueueCurationIfSegmentReady } from './segment-curation-readiness';

function createClient(rows: Array<{ ready: boolean; enqueued: boolean }>) {
  return {
    query: vi.fn().mockResolvedValue({ rowCount: rows.length, rows })
  } as unknown as PoolClient & { query: ReturnType<typeof vi.fn> };
}

describe('segment curation readiness', () => {
  it('enqueues only after all extraction rows for the segment are settled', async () => {
    const client = createClient([{ ready: false, enqueued: false }]);

    await expect(enqueueCurationIfSegmentReady(client, {
      vaultId: 'vault-1',
      segmentId: 'segment-1',
      enqueue: true
    })).resolves.toEqual({ ready: false, enqueued: false });

    const sql = String(client.query.mock.calls[0]?.[0]);
    expect(sql).toContain('NOT EXISTS');
    expect(sql).toContain('FROM extraction_queue eq');
    expect(sql).toContain('eq.segment_id = s.id');
    expect(sql).toContain('COALESCE(rc.processed, false) = false');
    expect(sql).toContain('FROM extraction_dead_letter edl');
    expect(sql).toContain('AND edl.segment_id = s.id');
    expect(sql).toContain('AND edl.chunk_id IS NULL');
    expect(sql).toContain('AND edl.chunk_id = segment_chunks.chunk_id');
    expect(sql).not.toContain('OR (edl.segment_id = s.id AND edl.chunk_id IS NULL)');
  });

  it('marks readiness and idempotently inserts the curation queue row', async () => {
    const client = createClient([{ ready: true, enqueued: true }]);

    await expect(enqueueCurationIfSegmentReady(client, {
      vaultId: 'vault-1',
      segmentId: 'segment-1',
      enqueue: true
    })).resolves.toEqual({ ready: true, enqueued: true });

    const sql = String(client.query.mock.calls[0]?.[0]);
    expect(sql).toContain('curation_ready_at = COALESCE');
    expect(sql).toContain('INSERT INTO curation_queue');
    expect(sql).toContain('ON CONFLICT (vault_id, segment_id) DO NOTHING');
    expect(client.query.mock.calls[0]?.[1]).toEqual(['vault-1', 'segment-1', true]);
  });
});
