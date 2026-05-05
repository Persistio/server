import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig, getConfiguredEmbeddingDimensions } from '../config';
import { query } from '../db/client';
import { createApiKey, requireAdminAuth } from '../middleware/auth';
import { generateAndWrapDek } from '../services/crypto';

const createVaultSchema = z.object({
  name: z.string().min(1)
});

export async function registerAdminRoutes(app: FastifyInstance) {
  app.post('/admin/vaults', { preHandler: requireAdminAuth }, async (request, reply) => {
    const body = createVaultSchema.parse(request.body);
    const apiKey = createApiKey();
    const encryptedDek = getConfig().ENCRYPTION_ENABLED
      ? (await generateAndWrapDek()).encryptedDek
      : null;

    const result = await query<{ id: string; name: string; created_at: string }>(
      `INSERT INTO vaults (name, api_key_hash, settings, encrypted_dek, vault_encryption_enabled)
       VALUES ($1, $2, $3::jsonb, $4, $5)
       RETURNING id, name, created_at`,
      [
        body.name,
        apiKey.hash,
        JSON.stringify({
          embedding_dimensions: getConfiguredEmbeddingDimensions()
        }),
        encryptedDek,
        encryptedDek !== null
      ]
    );

    return reply.code(201).send({
      id: result.rows[0].id,
      api_key: apiKey.rawKey
    });
  });

  app.post('/admin/vaults/:id/rotate-key', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const apiKey = createApiKey();
    const result = await query<{ id: string }>(
      `UPDATE vaults SET api_key_hash = $1 WHERE id = $2 RETURNING id`,
      [apiKey.hash, id]
    );
    if (!result.rowCount) return reply.code(404).send({ error: 'Vault not found' });
    return reply.code(200).send({ id: result.rows[0].id, api_key: apiKey.rawKey });
  });

  app.delete('/admin/vaults/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `DELETE FROM vaults
       WHERE id = $1
       RETURNING id`,
      [params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    return { id: result.rows[0].id, deleted: true };
  });

  app.get('/admin/vaults', { preHandler: requireAdminAuth }, async () => {
    const result = await query(
      `SELECT id, name, created_at, settings, plan_id, account_id, vault_encryption_enabled
       FROM vaults
       ORDER BY created_at DESC`
    );
    return { items: result.rows };
  });
}
