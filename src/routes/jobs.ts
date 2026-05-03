import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { requireTenantAuth } from '../middleware/auth';

export interface JobRecord {
  id: string;
  tenantId: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  createdAt: string;
  updatedAt: string;
  error?: string;
}

export interface JobStore {
  create(tenantId: string): JobRecord;
  get(jobId: string): JobRecord | undefined;
  update(jobId: string, status: JobRecord['status'], error?: string): JobRecord | undefined;
}

export async function registerJobRoutes(app: FastifyInstance, jobs: JobStore, triggerExtraction: (jobId: string, tenantId?: string) => void) {
  app.post('/v1/extract', { preHandler: requireTenantAuth }, async (request, reply) => {
    const job = jobs.create(request.tenant.id);
    triggerExtraction(job.id, request.tenant.id);
    return reply.code(202).send({ job_id: job.id });
  });

  app.get('/v1/jobs/:id', { preHandler: requireTenantAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const job = jobs.get(params.id);

    if (!job || job.tenantId !== request.tenant.id) {
      return reply.code(404).send({ error: 'Job not found' });
    }

    return job;
  });
}
