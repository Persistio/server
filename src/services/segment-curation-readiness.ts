import type { PoolClient } from 'pg';

export interface SegmentCurationReadinessResult {
  enqueued: boolean;
  ready: boolean;
}

export async function enqueueCurationIfSegmentReady(
  client: PoolClient,
  input: {
    vaultId: string;
    segmentId: string;
    enqueue: boolean;
  }
): Promise<SegmentCurationReadinessResult> {
  const result = await client.query<{ ready: boolean; enqueued: boolean }>(
    `WITH ready_segment AS (
       SELECT s.id, s.vault_id
       FROM segments s
       WHERE s.id = $2
         AND s.vault_id = $1
         AND cardinality(s.chunk_ids) > 0
         AND NOT EXISTS (
           SELECT 1
           FROM extraction_queue eq
           WHERE eq.vault_id = s.vault_id
             AND eq.segment_id = s.id
         )
         AND NOT EXISTS (
           SELECT 1
           FROM extraction_dead_letter edl
           WHERE edl.vault_id = s.vault_id
             AND edl.segment_id = s.id
             AND edl.chunk_id IS NULL
         )
         AND NOT EXISTS (
           SELECT 1
           FROM unnest(s.chunk_ids) AS segment_chunks(chunk_id)
           LEFT JOIN raw_chunks rc
             ON rc.id = segment_chunks.chunk_id
           WHERE COALESCE(rc.processed, false) = false
             AND NOT EXISTS (
               SELECT 1
               FROM extraction_dead_letter edl
               WHERE edl.vault_id = s.vault_id
                 AND edl.chunk_id = segment_chunks.chunk_id
             )
         )
       FOR UPDATE
     ),
     marked AS (
       UPDATE segments s
       SET curation_ready_at = COALESCE(s.curation_ready_at, now())
       FROM ready_segment
       WHERE s.id = ready_segment.id
       RETURNING s.id, s.vault_id
     ),
     inserted AS (
       INSERT INTO curation_queue (vault_id, segment_id)
       SELECT vault_id, id
       FROM marked
       WHERE $3::boolean
       ON CONFLICT (vault_id, segment_id) DO NOTHING
       RETURNING segment_id
     ),
     enqueued_mark AS (
       UPDATE segments s
       SET curation_enqueued_at = COALESCE(s.curation_enqueued_at, now())
       FROM marked
       WHERE s.id = marked.id
         AND $3::boolean
         AND (
           EXISTS (SELECT 1 FROM inserted)
           OR EXISTS (
             SELECT 1
             FROM curation_queue cq
             WHERE cq.vault_id = marked.vault_id
               AND cq.segment_id = marked.id
           )
         )
       RETURNING s.id
     )
     SELECT
       EXISTS (SELECT 1 FROM marked) AS ready,
       EXISTS (SELECT 1 FROM inserted) AS enqueued`,
    [input.vaultId, input.segmentId, input.enqueue]
  );

  return {
    ready: Boolean(result.rows[0]?.ready),
    enqueued: Boolean(result.rows[0]?.enqueued)
  };
}
