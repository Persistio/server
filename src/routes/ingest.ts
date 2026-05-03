import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { query } from '../db/client';
import { ingestChunksCounter } from '../metrics';
import { requireTenantAuth } from '../middleware/auth';
import { withSpan } from '../telemetry';

const ingestSchema = z.object({
  session_id: z.string().min(1),
  chunks: z.array(z.object({
    role: z.enum(['user', 'assistant', 'tool']),
    content: z.string().min(1)
  })).min(1)
});

export async function registerIngestRoutes(app: FastifyInstance) {
  app.post('/v1/ingest', { preHandler: requireTenantAuth }, async (request, reply) => {
    const body = ingestSchema.parse(request.body);
    return withSpan('ingest.request', {
      'tenant.id': request.tenant.id,
      'ingest.chunks_count': body.chunks.length,
      'ingest.session_id': body.session_id
    }, async (span) => {
      const inserted = await Promise.all(body.chunks.map(async (chunk) => {
        const result = await query<{ id: string; created_at: string }>(
          `INSERT INTO raw_chunks (tenant_id, session_id, role, content)
           VALUES ($1, $2, $3, $4)
           RETURNING id, created_at`,
          [request.tenant.id, body.session_id, chunk.role, chunk.content]
        );
        return result.rows[0];
      }));

      ingestChunksCounter.add(inserted.length, {
        tenant_id: request.tenant.id,
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
