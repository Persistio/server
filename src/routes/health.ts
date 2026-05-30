import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import type { AppConfig } from '../config';
import { pool } from '../db/client';

const HEALTH_DB_TIMEOUT_MS = 2000;
const serverPackageJsonPath = path.resolve(__dirname, '..', '..', 'package.json');
const serverVersion = JSON.parse(fs.readFileSync(serverPackageJsonPath, 'utf8')) as { version?: string };

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false;
  }

  return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

async function requireHealthAuth(request: FastifyRequest, reply: FastifyReply, configuredKey: string) {
  if (!configuredKey) {
    return;
  }

  const headerKey = request.headers['x-health-key'];
  const candidate = typeof headerKey === 'string' ? headerKey : undefined;

  if (!candidate || !timingSafeEqual(candidate, configuredKey)) {
    return reply.code(401).send({ error: 'Unauthorized' });
  }
}

async function checkDatabase() {
  const startedAt = Date.now();
  const dbCheck = pool.query('SELECT 1');
  const queueDepthsCheck = pool.query<{
    extraction_depth: number;
    curation_depth: number;
  }>(
    `SELECT
       (SELECT COUNT(*) FROM extraction_queue)::int AS extraction_depth,
       (SELECT COUNT(*) FROM curation_queue)::int AS curation_depth`
  );
  let timeoutHandle: NodeJS.Timeout | undefined;

  void dbCheck.catch(() => undefined);
  void queueDepthsCheck.catch(() => undefined);

  try {
    await Promise.race([
      dbCheck,
      new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => reject(new Error('Health check timed out')), HEALTH_DB_TIMEOUT_MS);
        timeoutHandle.unref();
      })
    ]);

    clearTimeout(timeoutHandle);
    let extractionQueueDepth: number | null = null;
    let curationQueueDepth: number | null = null;

    try {
      const result = await queueDepthsCheck;
      extractionQueueDepth = result.rows[0]?.extraction_depth ?? 0;
      curationQueueDepth = result.rows[0]?.curation_depth ?? 0;
    } catch {
      extractionQueueDepth = null;
      curationQueueDepth = null;
    }

    return {
      db: 'ok',
      db_latency_ms: Date.now() - startedAt,
      extraction_queue_depth: extractionQueueDepth,
      curation_queue_depth: curationQueueDepth
    } as const;
  } catch {
    clearTimeout(timeoutHandle);
    return {
      db: 'degraded',
      db_latency_ms: Date.now() - startedAt,
      extraction_queue_depth: null,
      curation_queue_depth: null
    } as const;
  }
}

export async function registerHealthRoutes(app: FastifyInstance, config: AppConfig) {
  app.get('/health', {
    preHandler: async (request, reply) => requireHealthAuth(request, reply, config.HEALTH_API_KEY)
  }, async (_request, reply) => {
    const database = await checkDatabase();
    const status = database.db === 'ok' ? 'ok' : 'degraded';

    return reply.code(status === 'ok' ? 200 : 503).send({
      status,
      version: serverVersion.version ?? '0.0.0',
      db: database.db,
      db_latency_ms: database.db_latency_ms,
      extraction_queue_depth: database.extraction_queue_depth,
      curation_queue_depth: database.curation_queue_depth,
      uptime_s: Math.round(process.uptime())
    });
  });
}
