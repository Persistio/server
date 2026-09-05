import type { FastifyInstance } from 'fastify';

import { requireVaultStatsAuth } from '../middleware/auth';
import { getVaultStats } from '../services/vault-stats';

export async function registerStatsRoutes(app: FastifyInstance) {
  app.get('/stats', { preHandler: requireVaultStatsAuth }, async (request, reply) => {
    const stats = await getVaultStats(request.vault.id);
    if (!stats) return reply.code(404).send({ error: 'Vault not found' });
    return stats;
  });
}
