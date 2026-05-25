import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { requireAdminAuth, requireVaultAuth } from '../middleware/auth';
import { getCurationStatus } from '../services/curation-capacity';

export async function registerCurationRoutes(app: FastifyInstance) {
  app.get('/v1/curation', { preHandler: requireVaultAuth }, async (request) => {
    return getCurationStatus(request.vault.id);
  });

  app.get('/admin/vaults/:id/curation', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);

    try {
      return await getCurationStatus(id);
    } catch (error) {
      if (error instanceof Error && error.message.includes('not found')) {
        return reply.code(404).send({ error: 'Vault not found' });
      }
      throw error;
    }
  });
}
