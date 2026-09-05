import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getAuthAccountId, requireAdminScope, requireVaultStatsAuth } from '../middleware/auth';
import { query } from '../db/client';
import { setCustomerMetricVaultId } from '../services/customer-api-request-metrics';
import { getCurationStatus } from '../services/curation-capacity';

export async function registerCurationRoutes(app: FastifyInstance) {
  app.get('/v1/curation', { preHandler: requireVaultStatsAuth }, async (request) => {
    return getCurationStatus(request.vault.id);
  });

  app.get('/admin/vaults/:id/curation', {
    preHandler: requireAdminScope('platform:vaults:read')
  }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const accountId = getAuthAccountId(request);
    const vaultAccess = await query<{ id: string }>(
      `SELECT id
       FROM vaults
       WHERE id = $1
         AND ($2::uuid IS NULL OR account_id = $2::uuid)
       LIMIT 1`,
      [id, accountId]
    );
    if (!vaultAccess.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    try {
      const status = await getCurationStatus(id);
      setCustomerMetricVaultId(request, id);
      return status;
    } catch (error) {
      if (error instanceof Error && error.message.includes('not found')) {
        return reply.code(404).send({ error: 'Vault not found' });
      }
      throw error;
    }
  });
}
