import crypto from 'node:crypto';

import type { FastifyInstance } from 'fastify';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { getConfig } from '../config';
import { withTransaction } from '../db/client';
import { ingestChunksCounter } from '../metrics';
import { requireVaultWriteAuth } from '../middleware/auth';
import type { VaultContext } from '../middleware/auth';
import { encryptForVault } from '../services/crypto';
import { recordCustomerMetric } from '../services/customer-metrics';
import { OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT, estimateEmbeddingTokens, getEmbedder } from '../services/embedder';
import { createRawChunkBlobKey, getRawChunkStorage, type RawChunkReference, type RawChunkStorage } from '../services/raw-chunk-storage';
import { applyRateLimitHeaders, consumeNormalIngestRateLimit, isPremiumPlan, refundApiQuotaReservation, reserveApiQuota, type ApiQuotaReservation } from '../services/usage';
import { withSpan } from '../telemetry';
import { cosineSimilarity } from '../utils/math';

const chunkSchema = z.object({
  role: z.enum(['user', 'assistant', 'tool']),
  content: z.string().min(1),
  timestamp: z.string().datetime({ offset: true }),
  provenance: z.object({
    source_class: z.enum([
      'agent_cron',
      'agent_hook',
      'agent_slack',
      'agent_subagent',
      'agent_other',
      'thread_conversation',
      'direct_or_import',
      'unknown'
    ]).optional(),
    actor_type: z.enum(['human', 'assistant', 'agent', 'tool', 'system', 'import', 'unknown']),
    trigger_type: z.enum(['direct', 'delegated', 'scheduled', 'event', 'backfill', 'api', 'unknown']),
    artifact_type: z.enum(['message', 'conversation', 'tool_result', 'status', 'observation', 'log', 'summary', 'document', 'unknown']),
    authorship: z.enum(['original', 'generated', 'transcribed', 'imported', 'mixed', 'unknown']),
    cadence: z.enum(['one_off', 'recurring', 'batch', 'unknown']),
    provenance_confidence: z.number().min(0).max(1).optional(),
    provenance_basis: z.array(z.enum([
      'session_id_prefix',
      'agent_trigger',
      'integration_marker',
      'thread_session_shape',
      'session_id_shape',
      'role_counts',
      'plugin_capture',
      'api_provenance',
      'api_provenance_aggregate',
      'fallback'
    ])).max(8).optional()
  }).optional()
});

export const ingestSchema = z.object({
  session_id: z.string().min(1),
  chunks: z.array(chunkSchema).min(1)
});

export const bulkIngestSchema = ingestSchema;

type IngestChunk = z.infer<typeof chunkSchema>;
type QueuePriority = 'normal' | 'bulk';

export async function registerIngestRoutes(
  app: FastifyInstance,
  triggerExtraction?: (jobId: string, vaultId?: string) => void
) {
  app.post('/v1/ingest', { preHandler: requireVaultWriteAuth }, async (request, reply) => {
    const body = ingestSchema.parse(request.body);
    const config = getConfig();
    if (body.chunks.length > config.MAX_INGEST_CHUNKS) {
      return reply.code(413).send({
        error: `Too many chunks: maximum is ${config.MAX_INGEST_CHUNKS}`
      });
    }
    const contentLimitError = getChunkContentLimitError(body.chunks, config.INGEST_CHUNK_MAX_CHARS, config.EMBEDDER_PROVIDER);
    if (contentLimitError) {
      return reply.code(413).send({ error: contentLimitError });
    }

    consumeNormalIngestRateLimit(request.vault.id, request.vault.plan_id, config.INGEST_RATE_LIMIT_RPM);
    const quotaReservation = await reserveApiQuota(request.vault.id, 'ingest_events', 'api');

    return withSpan('ingest.request', {
      'vault.id': request.vault.id,
      'ingest.chunks_count': body.chunks.length,
      'ingest.session_id': body.session_id
    }, async (span) => {
      const result = await ingestChunksWithQuotaReservation(
        quotaReservation,
        request.vault,
        body.session_id,
        body.chunks,
        'normal'
      );
      applyRateLimitHeaders(reply, quotaReservation.snapshot);

      ingestChunksCounter.add(result.inserted.length, {
        vault_id: request.vault.id,
        session_id: body.session_id
      });
      span.setAttribute('ingest.accepted', result.inserted.length);

      return reply.code(202).send({
        accepted: result.inserted.length,
        chunks: result.inserted
      });
    });
  });

  app.post('/v1/ingest/bulk', {
    preHandler: requireVaultWriteAuth,
    bodyLimit: getConfig().BULK_INGEST_BODY_LIMIT_BYTES
  }, async (request, reply) => {
    const body = bulkIngestSchema.parse(request.body);
    if (!isPremiumPlan(request.vault.plan_id)) {
      return reply.code(403).send({ error: 'Bulk ingest requires a premium plan' });
    }

    const config = getConfig();
    if (body.chunks.length > config.BULK_INGEST_MAX_CHUNKS) {
      return reply.code(413).send({
        error: `Too many chunks: maximum is ${config.BULK_INGEST_MAX_CHUNKS}`
      });
    }
    const contentLimitError = getChunkContentLimitError(body.chunks, config.INGEST_CHUNK_MAX_CHARS, config.EMBEDDER_PROVIDER);
    if (contentLimitError) {
      return reply.code(413).send({ error: contentLimitError });
    }
    const quotaReservation = await reserveApiQuota(request.vault.id, 'ingest_events', 'api');

    return withSpan('ingest.bulk_request', {
      'vault.id': request.vault.id,
      'ingest.chunks_count': body.chunks.length,
      'ingest.session_id': body.session_id
    }, async (span) => {
      const jobId = crypto.randomUUID();
      const result = await ingestChunksWithQuotaReservation(
        quotaReservation,
        request.vault,
        body.session_id,
        body.chunks,
        'bulk',
        jobId
      );
      applyRateLimitHeaders(reply, quotaReservation.snapshot);
      try {
        triggerExtraction?.(jobId, request.vault.id);
      } catch (error) {
        request.log.error({ err: error, jobId }, 'Failed to trigger extraction worker for bulk ingest job');
      }

      ingestChunksCounter.add(result.inserted.length, {
        vault_id: request.vault.id,
        session_id: body.session_id
      });
      span.setAttribute('ingest.accepted', result.inserted.length);
      span.setAttribute('ingest.segments_queued', result.segmentsQueued);

      return reply.code(202).send({
        accepted: result.inserted.length,
        chunks: result.inserted,
        job_id: jobId
      });
    });
  });
}

async function ingestChunksWithQuotaReservation(
  quotaReservation: ApiQuotaReservation,
  vault: VaultContext,
  sessionId: string,
  chunks: IngestChunk[],
  priority: QueuePriority,
  jobId?: string
): Promise<{ inserted: Array<{ id: string; created_at: string }>; segmentsQueued: number }> {
  try {
    return await ingestChunksForVault(vault, sessionId, chunks, priority, jobId);
  } catch (error) {
    try {
      await refundApiQuotaReservation(quotaReservation);
    } catch (refundError) {
      throw new AggregateError([error, refundError], 'Ingest failed and quota refund failed');
    }
    throw error;
  }
}

async function ingestChunksForVault(
  vault: VaultContext,
  sessionId: string,
  chunks: IngestChunk[],
  priority: QueuePriority,
  jobId?: string
): Promise<{ inserted: Array<{ id: string; created_at: string }>; segmentsQueued: number }> {
  const config = getConfig();
  const embedder = getEmbedder();
  const storage = getRawChunkStorage();
  const chunkIds = chunks.map(() => crypto.randomUUID());
  const embeddings = await embedder.embedBatch(
    chunks.map((chunk) => chunk.content),
    { vaultId: vault.id, modelRole: 'embedding', source: 'api', inputType: 'document' }
  );
  const storedContents = await Promise.all(
    chunks.map((chunk) => encryptForVault(vault, chunk.content))
  );
  const blobInputs = chunkIds.map((chunkId, index) => ({
    key: createRawChunkBlobKey(vault.id, sessionId, chunkId),
    content: storedContents[index],
    storageBytes: Buffer.byteLength(storedContents[index], 'utf8')
  }));
  const blobRefs = await putRawChunkBlobsWithRollback(storage, blobInputs);

  try {
    const result = await withTransaction(async (client) => {
      if (jobId) {
        await client.query(
          `INSERT INTO jobs (id, vault_id, kind, status)
           VALUES ($1, $2, 'bulk_ingest', 'queued')`,
          [jobId, vault.id]
        );
      }

      const inserted = await insertRawChunks(
        client,
        vault.id,
        sessionId,
        chunkIds,
        chunks,
        blobRefs,
        blobInputs.map((blob) => blob.storageBytes),
        embeddings
      );
      const insertedWithEmbeddings = inserted.map((insertedChunk, index) => ({
        ...insertedChunk,
        role: chunks[index].role,
        content: chunks[index].content,
        embedding: embeddings[index]
      }));
      const segments = buildSegments(
        insertedWithEmbeddings,
        config.SEGMENTATION_THRESHOLD
      );

      for (const segment of segments) {
        const segmentId = crypto.randomUUID();
        const storedContext = segment.context
          ? await encryptForVault(vault, segment.context)
          : null;
        await client.query(
          `INSERT INTO segments (id, vault_id, session_id, chunk_ids, context)
           VALUES ($1, $2, $3, $4::uuid[], $5)`,
          [segmentId, vault.id, sessionId, segment.chunkIds, storedContext]
        );
        await client.query(
          `INSERT INTO extraction_queue (segment_id, vault_id, priority, job_id)
           VALUES ($1, $2, $3, $4)`,
          [segmentId, vault.id, priority, jobId ?? null]
        );
      }

      return {
        inserted,
        segmentsQueued: segments.length
      };
    });
    recordRawChunkStorageDelta(vault, blobInputs);
    return result;
  } catch (error) {
    const deleteErrors = await deleteRawChunkBlobKeys(storage, blobInputs.map((blob) => blob.key));
    if (deleteErrors.length > 0) {
      throw new AggregateError([error, ...deleteErrors], 'Ingest failed and raw chunk blob cleanup failed');
    }
    throw error;
  }
}

function recordRawChunkStorageDelta(
  vault: VaultContext,
  blobs: Array<{ storageBytes: number }>
): void {
  if (!vault.account_id) return;
  const storageBytes = blobs.reduce((total, blob) => total + blob.storageBytes, 0);
  if (storageBytes === 0) return;

  recordCustomerMetric({
    event_type: 'storage_delta',
    operation: 'raw_chunk_blob_write',
    source: 'api',
    storage_bytes_delta: storageBytes,
    vault_id: vault.id,
    workspace_id: vault.account_id
  });
}

async function putRawChunkBlobsWithRollback(
  storage: RawChunkStorage,
  blobs: Array<{ key: string; content: string }>
): Promise<RawChunkReference[]> {
  const settled = await Promise.allSettled(
    blobs.map((blob) => storage.put(blob.key, blob.content))
  );
  const refs: RawChunkReference[] = [];
  const errors: unknown[] = [];

  settled.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      refs[index] = result.value;
    } else {
      errors.push(result.reason);
    }
  });

  if (errors.length > 0) {
    const deleteErrors = await deleteRawChunkBlobKeys(storage, blobs.map((blob) => blob.key));
    if (deleteErrors.length > 0) {
      throw new AggregateError([...errors, ...deleteErrors], 'Failed to store raw chunk blobs and clean up partial writes');
    }
    throw new AggregateError(errors, 'Failed to store raw chunk blobs');
  }

  return refs;
}

async function deleteRawChunkBlobRefs(storage: RawChunkStorage, refs: RawChunkReference[]): Promise<unknown[]> {
  return deleteRawChunkBlobKeys(storage, refs.map((ref) => ref.blobKey));
}

async function deleteRawChunkBlobKeys(storage: RawChunkStorage, keys: string[]): Promise<unknown[]> {
  const settled = await Promise.allSettled(keys.map((key) => storage.delete(key)));
  return settled.flatMap((result) => result.status === 'rejected' ? [result.reason] : []);
}

function getChunkContentLimitError(chunks: IngestChunk[], maxChars: number, embedderProvider: string): string | undefined {
  const oversizedIndex = chunks.findIndex((chunk) => chunk.content.length > maxChars);
  if (oversizedIndex !== -1) {
    return `Chunk ${oversizedIndex} content is too large: maximum is ${maxChars} characters`;
  }

  if (embedderProvider !== 'openai') {
    return undefined;
  }

  const providerLimitIndex = chunks.findIndex((chunk) =>
    estimateEmbeddingTokens(chunk.content) > OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT
  );
  if (providerLimitIndex !== -1) {
    return `Chunk ${providerLimitIndex} content is too large for embedding: estimated token maximum is ${OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT}`;
  }

  return undefined;
}

async function insertRawChunks(
  client: PoolClient,
  vaultId: string,
  sessionId: string,
  chunkIds: string[],
  chunks: IngestChunk[],
  blobRefs: RawChunkReference[],
  storageBytes: number[],
  embeddings: number[][]
): Promise<Array<{ id: string; created_at: string }>> {
  const result = await client.query<{ id: string; created_at: string }>(
    `WITH input AS (
       SELECT *
       FROM UNNEST($3::uuid[], $4::text[], $5::text[], $6::text[], $7::bigint[], $8::text[], $9::timestamptz[], $10::jsonb[]) WITH ORDINALITY
         AS input(id, role, blob_store, blob_key, storage_bytes, embedding, created_at, provenance, ordinal)
     ),
     inserted AS (
       INSERT INTO raw_chunks (id, vault_id, session_id, role, blob_store, blob_key, storage_bytes, embedding, created_at, provenance)
       SELECT input.id, $1, $2, input.role, input.blob_store, input.blob_key, input.storage_bytes, input.embedding::vector, input.created_at, input.provenance
       FROM input
       ORDER BY input.ordinal
       RETURNING id, created_at
     )
     SELECT inserted.id, inserted.created_at
     FROM input
     JOIN inserted
       ON inserted.id = input.id
     ORDER BY input.ordinal`,
    [
      vaultId,
      sessionId,
      chunkIds,
      chunks.map((chunk) => chunk.role),
      blobRefs.map((ref) => ref.blobStore),
      blobRefs.map((ref) => ref.blobKey),
      storageBytes,
      embeddings.map((embedding) => JSON.stringify(embedding)),
      chunks.map((chunk) => chunk.timestamp),
      chunks.map((chunk) => chunk.provenance ? JSON.stringify(chunk.provenance) : null)
    ]
  );

  if (result.rows.length !== chunks.length) {
    throw new Error(`Expected ${chunks.length} inserted chunks but got ${result.rows.length}`);
  }

  return result.rows;
}

interface InsertedChunk {
  id: string;
  role: string;
  content: string;
  embedding: number[];
}

interface SegmentDraft {
  chunkIds: string[];
  context: string | null;
}

function buildSegments(
  chunks: InsertedChunk[],
  threshold: number
): SegmentDraft[] {
  const minSize = 3;
  const maxSize = 40;
  if (!chunks.length) {
    return [];
  }

  const drafts: InsertedChunk[][] = [];
  let current: InsertedChunk[] = [chunks[0]];

  for (let index = 1; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    const similarity = cosineSimilarity(current[current.length - 1].embedding, chunk.embedding);
    const remaining = chunks.length - index;
    const shouldSplitForSimilarity = similarity < threshold && current.length >= minSize && remaining >= minSize;
    const shouldSplitForSize = current.length >= maxSize;

    if (shouldSplitForSimilarity || shouldSplitForSize) {
      drafts.push(current);
      current = [chunk];
      continue;
    }

    current.push(chunk);
  }

  if (current.length < minSize && drafts.length) {
    drafts[drafts.length - 1].push(...current);
  } else {
    drafts.push(current);
  }

  return drafts.map((draft) => ({
    chunkIds: draft.map((chunk) => chunk.id),
    context: buildSegmentContext(draft)
  }));
}

function buildSegmentContext(chunks: InsertedChunk[]): string | null {
  const preview = chunks
    .map((chunk) => chunk.content.trim())
    .find((content) => content.length > 0);

  return preview ? preview.slice(0, 280) : null;
}
