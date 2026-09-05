import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { query } from '../db/client';
import { requireVaultReadAuth, requireVaultWriteAuth } from '../middleware/auth';

export interface JobRecord {
  id: string;
  vaultId: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  createdAt: string;
  updatedAt: string;
  error?: string;
}

export interface JobStore {
  create(vaultId: string): JobRecord;
  get(jobId: string): JobRecord | undefined;
  update(jobId: string, status: JobRecord['status'], error?: string): JobRecord | undefined;
}

export async function registerJobRoutes(app: FastifyInstance, jobs: JobStore, triggerExtraction: (jobId: string, vaultId?: string) => void) {
  app.post('/v1/extract', { preHandler: requireVaultWriteAuth }, async (request, reply) => {
    const job = jobs.create(request.vault.id);
    triggerExtraction(job.id, request.vault.id);
    return reply.code(202).send({ job_id: job.id });
  });

  app.get('/v1/jobs/:id', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const job = jobs.get(params.id);

    if (job && job.vaultId === request.vault.id) {
      return job;
    }

    const persistentJob = await getPersistentJob(params.id, request.vault.id);
    if (!persistentJob) {
      return reply.code(404).send({ error: 'Job not found' });
    }

    return persistentJob;
  });
}

async function getPersistentJob(jobId: string, vaultId: string): Promise<JobRecord | undefined> {
  const result = await query<{
    id: string;
    vaultId: string;
    status: JobRecord['status'];
    createdAt: string;
    updatedAt: string;
    error: string | null;
  }>(
    `SELECT id,
            vault_id AS "vaultId",
            status,
            created_at AS "createdAt",
            updated_at AS "updatedAt",
            error
     FROM jobs
     WHERE id = $1
       AND vault_id = $2
     LIMIT 1`,
    [jobId, vaultId]
  );

  if (!result.rowCount) {
    return undefined;
  }

  return {
    ...result.rows[0],
    error: result.rows[0].error ?? undefined
  };
}
