import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import type { AppConfig } from '../config';
import { EXTRACTION_QUEUE_READY_PREDICATE } from '../services/extraction-queue-eligibility';

const HEALTH_DB_TIMEOUT_MS = 2000;
const serverPackageJsonPath = path.resolve(__dirname, '..', '..', 'package.json');
const serverVersion = JSON.parse(fs.readFileSync(serverPackageJsonPath, 'utf8')) as { version?: string };

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timeoutHandle: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timeoutHandle = setTimeout(() => reject(new Error('Health check timed out')), timeoutMs);
    timeoutHandle.unref();
  });

  return Promise.race([promise, timeout]).finally(() => clearTimeout(timeoutHandle));
}

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

export async function checkDatabase(timeoutMs = HEALTH_DB_TIMEOUT_MS) {
  const { pool } = await import('../db/client');
  const startedAt = Date.now();
  const dbCheck = withTimeout(pool.query('SELECT 1'), timeoutMs);
  const queueDepthsCheck = withTimeout(pool.query<{
    extraction_depth: number;
    extraction_inflight_depth: number;
    curation_depth: number;
  }>(
    `SELECT
       (SELECT COUNT(*) FROM extraction_queue eq WHERE ${EXTRACTION_QUEUE_READY_PREDICATE})::int AS extraction_depth,
       (SELECT COUNT(*) FROM extraction_queue WHERE claimed_at IS NOT NULL)::int AS extraction_inflight_depth,
       (SELECT COUNT(*) FROM curation_queue)::int AS curation_depth`
  ), timeoutMs);

  void dbCheck.catch(() => undefined);
  void queueDepthsCheck.catch(() => undefined);

  try {
    await dbCheck;

    const result = await queueDepthsCheck.then((queueDepths) => ({
      extractionQueueDepth: queueDepths.rows[0]?.extraction_depth ?? 0,
      extractionInflightDepth: queueDepths.rows[0]?.extraction_inflight_depth ?? 0,
      curationQueueDepth: queueDepths.rows[0]?.curation_depth ?? 0
    }), () => ({
      extractionQueueDepth: null,
      extractionInflightDepth: null,
      curationQueueDepth: null
    }));

    return {
      db: 'ok',
      db_latency_ms: Date.now() - startedAt,
      extraction_queue_depth: result.extractionQueueDepth,
      extraction_inflight_depth: result.extractionInflightDepth,
      curation_queue_depth: result.curationQueueDepth
    } as const;
  } catch {
    return {
      db: 'degraded',
      db_latency_ms: Date.now() - startedAt,
      extraction_queue_depth: null,
      extraction_inflight_depth: null,
      curation_queue_depth: null
    } as const;
  }
}

export async function registerHealthRoutes(app: FastifyInstance, config: AppConfig) {
  app.get('/health', {
    preHandler: async (request, reply) => requireHealthAuth(request, reply, config.HEALTH_API_KEY)
  }, async (_request, reply) => {
    const database = config.PERSISTIO_MODE === 'analytics-api'
      ? {
        curation_queue_depth: null,
        db: 'not_applicable',
        db_latency_ms: 0,
        extraction_inflight_depth: null,
        extraction_queue_depth: null
      } as const
      : await checkDatabase();
    const status = database.db === 'ok' || database.db === 'not_applicable' ? 'ok' : 'degraded';

    return reply.code(status === 'ok' ? 200 : 503).send({
      status,
      version: serverVersion.version ?? '0.0.0',
      db: database.db,
      db_latency_ms: database.db_latency_ms,
      extraction_queue_depth: database.extraction_queue_depth,
      extraction_inflight_depth: database.extraction_inflight_depth,
      curation_queue_depth: database.curation_queue_depth,
      uptime_s: Math.round(process.uptime())
    });
  });
}
