import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { pool } from '../db/client';
import { ingestChunksCounter } from '../metrics';
import { requireVaultAuth } from '../middleware/auth';
import { encryptForVault } from '../services/crypto';
import { checkQuota, incrementUsage } from '../services/usage';
import { withSpan } from '../telemetry';

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
      const inserted: Array<{ id: string; created_at: string }> = [];

      for (const chunk of body.chunks) {
        const storedContent = await encryptForVault(request.vault, chunk.content);
        const client = await pool.connect();

        try {
          await client.query('BEGIN');
          const result = await client.query<{ id: string; created_at: string }>(
            `INSERT INTO raw_chunks (vault_id, session_id, role, content)
             VALUES ($1, $2, $3, $4)
             RETURNING id, created_at`,
            [request.vault.id, body.session_id, chunk.role, storedContent]
          );
          const insertedChunk = result.rows[0];
          await client.query(
            `INSERT INTO extraction_queue (chunk_id, vault_id)
             VALUES ($1, $2)`,
            [insertedChunk.id, request.vault.id]
          );
          await client.query('COMMIT');
          inserted.push(insertedChunk);
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        } finally {
          client.release();
        }
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
