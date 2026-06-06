export const EXTRACTION_QUEUE_READY_PREDICATE = `eq.claimed_at IS NULL
           AND eq.available_at <= now()
           AND (
             (
               eq.segment_id IS NOT NULL
               AND EXISTS (
                 SELECT 1
                 FROM segments s
                 WHERE s.id = eq.segment_id
                   AND cardinality(s.chunk_ids) > 0
                   AND NOT EXISTS (
                     SELECT 1
                     FROM unnest(s.chunk_ids) AS chunk_ids(chunk_id)
                     LEFT JOIN raw_chunks rc ON rc.id = chunk_ids.chunk_id
                     WHERE rc.blob_key IS NULL
                   )
               )
             )
             OR (
               eq.chunk_id IS NOT NULL
               AND EXISTS (
                 SELECT 1
                 FROM raw_chunks rc
                 WHERE rc.id = eq.chunk_id
                   AND rc.blob_key IS NOT NULL
               )
             )
           )`;
