import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig } from '../config';
import { query } from '../db/client';
import { recallDurationHistogram } from '../metrics';
import { requireVaultAuth } from '../middleware/auth';
import { decryptForVault } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { checkQuota, incrementUsage } from '../services/usage';
import { withSpan } from '../telemetry';

const recallSchema = z.object({
  query: z.string().min(1),
  top_k: z.number().int().positive().max(100).optional(),
  include_raw: z.boolean().optional().default(false)
});

export async function registerRecallRoutes(app: FastifyInstance) {
  app.post('/v1/recall', { preHandler: requireVaultAuth }, async (request) => {
    const body = recallSchema.parse(request.body);
    const config = getConfig();
    const topK = body.top_k ?? config.DEFAULT_RECALL_TOP_K;
    await checkQuota(request.vault.id, 'searches');

    return withSpan('recall.request', {
      'vault.id': request.vault.id,
      'recall.include_raw': body.include_raw,
      'recall.top_k': topK
    }, async (span) => {
      const start = performance.now();
      const embedder = getEmbedder();
      const embedding = await embedder.embed(body.query);

      const memoriesResult = await query<{
        id: string;
        data: string;
        subject: string;
        categories: string[];
        confidence: number;
        similarity: number;
        created_at: string;
        updated_at: string;
        recall_count: number;
        last_recalled: string | null;
      }>(
        `SELECT id, data, subject, categories, confidence, created_at, updated_at, recall_count, last_recalled,
                1 - (embedding <=> $2::vector) AS similarity
         FROM memories
         WHERE vault_id = $1
           AND archived_at IS NULL
           AND embedding IS NOT NULL
         ORDER BY embedding <=> $2::vector
         LIMIT $3`,
        [request.vault.id, JSON.stringify(embedding), topK]
      );

      if (memoriesResult.rowCount) {
        await query(
          `UPDATE memories
           SET last_recalled = now(),
               recall_count = recall_count + 1
           WHERE id = ANY($1::uuid[])`,
          [memoriesResult.rows.map((row) => row.id)]
        );
      }

      let rawChunks: Array<Record<string, unknown>> = [];

      if (body.include_raw) {
        const rawResult = await query<{
          id: string;
          session_id: string;
          role: string;
          content: string;
          similarity: number;
          created_at: string;
        }>(
          `SELECT id, session_id, role, content, created_at,
                  1 - (embedding <=> $2::vector) AS similarity
           FROM raw_chunks
           WHERE vault_id = $1
             AND embedding IS NOT NULL
           ORDER BY embedding <=> $2::vector
           LIMIT $3`,
          [request.vault.id, JSON.stringify(embedding), topK]
        );
        rawChunks = rawResult.rows;
      }

      const decryptedMemories = await Promise.all(memoriesResult.rows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedRawChunks = await Promise.all(rawChunks.map(async (row) => ({
        ...row,
        content: typeof row.content === 'string' ? await decryptForVault(request.vault, row.content) : row.content
      })));

      await incrementUsage(request.vault.id, 'searches');

      const durationMs = performance.now() - start;
      recallDurationHistogram.record(durationMs, {
        vault_id: request.vault.id,
        include_raw: String(body.include_raw)
      });
      span.setAttribute('recall.results_returned', memoriesResult.rows.length);
      span.setAttribute('recall.raw_results_returned', rawChunks.length);
      span.setAttribute('recall.duration_ms', durationMs);

      return {
        memories: decryptedMemories,
        raw_chunks: decryptedRawChunks
      };
    });
  });
}
