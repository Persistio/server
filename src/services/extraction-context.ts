import type { PoolClient } from 'pg';
import type { RecallContext } from './memory-applicability';

export const MAX_EXTRACTION_CONTEXT_CHUNKS = 8;
export const MAX_EXTRACTION_CONTEXT_BYTES = 32 * 1024;
export interface AcceptedContextChunk {
  id: string; vault_id: string; session_id: string; role: string;
  blob_store: string | null; blob_key: string | null; created_at: string;
  provenance: unknown; storage_bytes: string;
}

/** The caller holds the job lease transaction. Freeze once, before provider IO. */
export async function freezeExtractionContext(client: PoolClient, input: {
  queueId: string; vaultId: string; chunkIds: string[]; context: RecallContext; blobStore: string;
}): Promise<AcceptedContextChunk[]> {
  const job = await client.query<{ context_chunk_ids: string[] | null }>(
    'SELECT context_chunk_ids FROM extraction_queue WHERE id=$1 AND vault_id=$2 FOR UPDATE',
    [input.queueId,input.vaultId]
  );
  if (!job.rows.length) throw new Error('Extraction job no longer exists');
  let ids = job.rows[0].context_chunk_ids;
  if (ids === null) {
    const previous = await client.query<AcceptedContextChunk>(
      `SELECT rc.id,rc.vault_id,rc.session_id,rc.role,rc.blob_store,rc.blob_key,rc.created_at,
              rc.provenance,rc.storage_bytes::text
       FROM raw_chunks rc
       WHERE rc.vault_id=$1 AND rc.session_id=$2 AND rc.role IN ('user','assistant')
         AND (rc.capture_context->>'project_id'=$4::text OR ($4::text IS NULL AND rc.capture_context->>'project_id' IS NULL))
         AND (rc.capture_context->>'task_id'=$5::text OR ($5::text IS NULL AND rc.capture_context->>'task_id' IS NULL))
         AND rc.acceptance_ordinal < (SELECT min(acceptance_ordinal) FROM raw_chunks WHERE vault_id=$1 AND id=ANY($3::uuid[]))
         AND rc.storage_bytes BETWEEN 0 AND $6 AND rc.blob_key IS NOT NULL
         AND (rc.blob_store IS NULL OR rc.blob_store=$8)
       ORDER BY rc.acceptance_ordinal DESC LIMIT $7`,
      [input.vaultId,input.context.session_id,input.chunkIds,input.context.project_id ?? null,
        input.context.task_id ?? null,MAX_EXTRACTION_CONTEXT_BYTES,MAX_EXTRACTION_CONTEXT_CHUNKS,input.blobStore]
    );
    let bytes = 0;
    ids = previous.rows.filter(chunk => {
      const size = Number(chunk.storage_bytes);
      if (!Number.isSafeInteger(size) || size < 0 || bytes + size > MAX_EXTRACTION_CONTEXT_BYTES) return false;
      bytes += size;
      return true;
    }).reverse().map(chunk => chunk.id);
    await client.query('UPDATE extraction_queue SET context_chunk_ids=$3::uuid[] WHERE id=$1 AND vault_id=$2',
      [input.queueId,input.vaultId,ids]);
  }
  const rows = await client.query<AcceptedContextChunk>(
    `SELECT id,vault_id,session_id,role,blob_store,blob_key,created_at,provenance,storage_bytes::text
     FROM raw_chunks WHERE vault_id=$1 AND session_id=$2 AND id=ANY($3::uuid[])
       AND role IN ('user','assistant') AND storage_bytes BETWEEN 0 AND $4 AND blob_key IS NOT NULL
       AND (blob_store IS NULL OR blob_store=$7)
       AND capture_context->>'project_id' IS NOT DISTINCT FROM $5::text
       AND capture_context->>'task_id' IS NOT DISTINCT FROM $6::text`,
    [input.vaultId,input.context.session_id,ids,MAX_EXTRACTION_CONTEXT_BYTES,input.context.project_id ?? null,input.context.task_id ?? null,input.blobStore]
  );
  const byId = new Map(rows.rows.map(row => [row.id,row]));
  // A deleted context record is absence of context, never permission to discover
  // different history on a retry. Current evidence remains independently required.
  return ids.flatMap(id => byId.has(id) ? [byId.get(id)!] : []);
}
