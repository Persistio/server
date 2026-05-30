import crypto from 'node:crypto';

import type { FastifyInstance } from 'fastify';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { getConfig } from '../config';
import { withTransaction } from '../db/client';
import { ingestChunksCounter } from '../metrics';
import { requireVaultAuth } from '../middleware/auth';
import type { VaultContext } from '../middleware/auth';
import { encryptForVault } from '../services/crypto';
import { OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT, estimateEmbeddingTokens, getEmbedder } from '../services/embedder';
import { applyRateLimitHeaders, consumeNormalIngestRateLimit, isPremiumPlan, refundApiQuotaReservation, reserveApiQuota, type ApiQuotaReservation } from '../services/usage';
import { withSpan } from '../telemetry';
import { cosineSimilarity } from '../utils/math';

const chunkSchema = z.object({
  role: z.enum(['user', 'assistant', 'tool']),
  content: z.string().min(1),
  timestamp: z.string().datetime({ offset: true })
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
  app.post('/v1/ingest', { preHandler: requireVaultAuth }, async (request, reply) => {
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
    const quotaReservation = await reserveApiQuota(request.vault.id, 'ingest_events');

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
    preHandler: requireVaultAuth,
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
    const quotaReservation = await reserveApiQuota(request.vault.id, 'ingest_events');

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
  const embeddings = await embedder.embedBatch(chunks.map((chunk) => chunk.content));
  const storedContents = await Promise.all(
    chunks.map((chunk) => encryptForVault(vault, chunk.content))
  );
  return withTransaction(async (client) => {
    if (jobId) {
      await client.query(
        `INSERT INTO jobs (id, vault_id, kind, status)
         VALUES ($1, $2, 'bulk_ingest', 'queued')`,
        [jobId, vault.id]
      );
    }

    const inserted = await insertRawChunks(client, vault.id, sessionId, chunks, storedContents, embeddings);
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
  chunks: IngestChunk[],
  storedContents: string[],
  embeddings: number[][]
): Promise<Array<{ id: string; created_at: string }>> {
  const chunkIds = chunks.map(() => crypto.randomUUID());
  const result = await client.query<{ id: string; created_at: string }>(
    `WITH input AS (
       SELECT *
       FROM UNNEST($3::uuid[], $4::text[], $5::text[], $6::text[], $7::timestamptz[]) WITH ORDINALITY
         AS input(id, role, content, embedding, created_at, ordinal)
     ),
     inserted AS (
       INSERT INTO raw_chunks (id, vault_id, session_id, role, content, embedding, created_at)
       SELECT input.id, $1, $2, input.role, input.content, input.embedding::vector, input.created_at
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
      storedContents,
      embeddings.map((embedding) => JSON.stringify(embedding)),
      chunks.map((chunk) => chunk.timestamp)
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
