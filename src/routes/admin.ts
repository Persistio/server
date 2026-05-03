import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfiguredEmbeddingDimensions } from '../config';
import { query } from '../db/client';
import { createApiKey, requireAdminAuth } from '../middleware/auth';

const createTenantSchema = z.object({
  name: z.string().min(1)
});

export async function registerAdminRoutes(app: FastifyInstance) {
  app.post('/admin/tenants', { preHandler: requireAdminAuth }, async (request, reply) => {
    const body = createTenantSchema.parse(request.body);
    const apiKey = createApiKey();
    const result = await query<{ id: string; name: string; created_at: string }>(
      `INSERT INTO tenants (name, api_key_hash, settings)
       VALUES ($1, $2, $3::jsonb)
       RETURNING id, name, created_at`,
      [
        body.name,
        apiKey.hash,
        JSON.stringify({
          embedding_dimensions: getConfiguredEmbeddingDimensions()
        })
      ]
    );

    return reply.code(201).send({
      id: result.rows[0].id,
      api_key: apiKey.rawKey
    });
  });

  app.delete('/admin/tenants/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `DELETE FROM tenants
       WHERE id = $1
       RETURNING id`,
      [params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Tenant not found' });
    }

    return { id: result.rows[0].id, deleted: true };
  });

  app.get('/admin/tenants', { preHandler: requireAdminAuth }, async () => {
    const result = await query(
      `SELECT id, name, created_at, settings
       FROM tenants
       ORDER BY created_at DESC`
    );
    return { items: result.rows };
  });
}
