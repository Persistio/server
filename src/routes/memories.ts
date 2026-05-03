import crypto from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { query } from '../db/client';
import { requireTenantAuth } from '../middleware/auth';
import { getEmbedder } from '../services/embedder';

const listQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  category: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0)
});

const createMemorySchema = z.object({
  data: z.string().min(1),
  subject: z.string().min(1),
  categories: z.array(z.string().min(1)).optional().default([])
});

const updateMemorySchema = z.object({
  data: z.string().min(1).optional(),
  subject: z.string().min(1).optional(),
  categories: z.array(z.string().min(1)).optional(),
  confidence: z.number().positive().optional()
});

export async function registerMemoryRoutes(app: FastifyInstance) {
  app.get('/v1/memories', { preHandler: requireTenantAuth }, async (request) => {
    const qs = listQuerySchema.parse(request.query);
    const values: unknown[] = [request.tenant.id];
    const conditions = ['tenant_id = $1'];

    if (qs.archived === 'false') {
      conditions.push('archived_at IS NULL');
    } else {
      conditions.push('archived_at IS NOT NULL');
    }

    if (qs.category) {
      values.push(qs.category);
      conditions.push(`$${values.length} = ANY(categories)`);
    }

    values.push(qs.limit, qs.offset);

    const result = await query(
      `SELECT *
       FROM memories
       WHERE ${conditions.join(' AND ')}
       ORDER BY updated_at DESC, created_at DESC
       LIMIT $${values.length - 1}
       OFFSET $${values.length}`,
      values
    );

    return {
      items: result.rows,
      limit: qs.limit,
      offset: qs.offset
    };
  });

  app.post('/v1/memories', { preHandler: requireTenantAuth }, async (request, reply) => {
    const body = createMemorySchema.parse(request.body);
    const embedder = getEmbedder();
    const embedding = await embedder.embed(body.data);
    const hash = crypto.createHash('md5').update(body.data).digest('hex');

    const result = await query(
      `INSERT INTO memories (tenant_id, data, subject, hash, embedding, categories)
       VALUES ($1, $2, $3, $4, $5::vector, $6::text[])
       RETURNING *`,
      [request.tenant.id, body.data, body.subject, hash, JSON.stringify(embedding), body.categories]
    );

    return reply.code(201).send(result.rows[0]);
  });

  app.get('/v1/memories/:id', { preHandler: requireTenantAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `SELECT *
       FROM memories
       WHERE tenant_id = $1 AND id = $2
       LIMIT 1`,
      [request.tenant.id, params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    return result.rows[0];
  });

  app.delete('/v1/memories/:id', { preHandler: requireTenantAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `UPDATE memories
       SET archived_at = now(),
           updated_at = now()
       WHERE tenant_id = $1 AND id = $2
       RETURNING id, archived_at`,
      [request.tenant.id, params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    return result.rows[0];
  });

  app.patch('/v1/memories/:id', { preHandler: requireTenantAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const body = updateMemorySchema.parse(request.body);

    const existing = await query<{
      id: string;
      data: string;
      subject: string;
      categories: string[];
      confidence: number;
    }>(
      `SELECT id, data, subject, categories, confidence
       FROM memories
       WHERE tenant_id = $1 AND id = $2
       LIMIT 1`,
      [request.tenant.id, params.id]
    );

    if (!existing.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    const current = existing.rows[0];
    const nextData = body.data ?? current.data;
    const nextSubject = body.subject ?? current.subject;
    const nextCategories = body.categories ?? current.categories;
    const nextConfidence = body.confidence ?? current.confidence;

    let embedding = undefined as string | undefined;
    let hash = undefined as string | undefined;

    if (body.data) {
      const embedder = getEmbedder();
      embedding = JSON.stringify(await embedder.embed(nextData));
      hash = crypto.createHash('md5').update(nextData).digest('hex');
    }

    const result = await query(
      `UPDATE memories
       SET data = $3,
           subject = $4,
           categories = $5::text[],
           confidence = $6,
           updated_at = now(),
           hash = COALESCE($7, hash),
           embedding = COALESCE($8::vector, embedding)
       WHERE tenant_id = $1 AND id = $2
       RETURNING *`,
      [
        request.tenant.id,
        params.id,
        nextData,
        nextSubject,
        nextCategories,
        nextConfidence,
        hash,
        embedding
      ]
    );

    return result.rows[0];
  });
}
