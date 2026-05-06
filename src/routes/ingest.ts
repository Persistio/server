import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig } from '../config';
import { pool } from '../db/client';
import { ingestChunksCounter } from '../metrics';
import { requireVaultAuth } from '../middleware/auth';
import { encryptForVault } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { checkQuota, incrementUsage } from '../services/usage';
import { withSpan } from '../telemetry';
import { cosineSimilarity } from '../utils/math';

const ingestSchema = z.object({
  session_id: z.string().min(1),
  chunks: z.array(z.object({
    role: z.enum(['user', 'assistant', 'tool']),
    content: z.string().min(1)
  })).min(1)
});

export async function registerIngestRoutes(app: FastifyInstance) {
  app.post('/v1/ingest', { preHandler: requireVaultAuth }, async (request, reply) => {
    const body = ingestSchema.parse(request.body);
    await checkQuota(request.vault.id, 'ingest_events');

    return withSpan('ingest.request', {
      'vault.id': request.vault.id,
      'ingest.chunks_count': body.chunks.length,
      'ingest.session_id': body.session_id
    }, async (span) => {
      const embedder = getEmbedder();
      const inserted: Array<{ id: string; created_at: string }> = [];
      const insertedWithEmbeddings: Array<{ id: string; created_at: string; role: string; content: string; embedding: number[] }> = [];
      const prepared = await Promise.all(body.chunks.map(async (chunk) => {
        const [storedContent, embedding] = await Promise.all([
          encryptForVault(request.vault, chunk.content),
          embedder.embed(chunk.content)
        ]);

        return {
          chunk,
          storedContent,
          embedding
        };
      }));
      const client = await pool.connect();

      try {
        await client.query('BEGIN');
        for (const { chunk, storedContent, embedding } of prepared) {
          const result = await client.query<{ id: string; created_at: string }>(
            `INSERT INTO raw_chunks (vault_id, session_id, role, content, embedding)
             VALUES ($1, $2, $3, $4, $5::vector)
             RETURNING id, created_at`,
            [request.vault.id, body.session_id, chunk.role, storedContent, JSON.stringify(embedding)]
          );
          const insertedChunk = result.rows[0];
          inserted.push(insertedChunk);
          insertedWithEmbeddings.push({
            ...insertedChunk,
            role: chunk.role,
            content: chunk.content,
            embedding
          });
        }
        const segments = buildSegments(
          insertedWithEmbeddings,
          getConfig().SEGMENTATION_THRESHOLD
        );

        for (const segment of segments) {
          const storedContext = segment.context
            ? await encryptForVault(request.vault, segment.context)
            : null;
          const segmentResult = await client.query<{ id: string }>(
            `INSERT INTO segments (vault_id, session_id, chunk_ids, context)
             VALUES ($1, $2, $3::uuid[], $4)
             RETURNING id`,
            [request.vault.id, body.session_id, segment.chunkIds, storedContext]
          );
          await client.query(
            `INSERT INTO extraction_queue (segment_id, vault_id)
             VALUES ($1, $2)`,
            [segmentResult.rows[0].id, request.vault.id]
          );
        }

        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

      await incrementUsage(request.vault.id, 'ingest_events');

      ingestChunksCounter.add(inserted.length, {
        vault_id: request.vault.id,
        session_id: body.session_id
      });
      span.setAttribute('ingest.accepted', inserted.length);

      return reply.code(202).send({
        accepted: inserted.length,
        chunks: inserted
      });
    });
  });
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
