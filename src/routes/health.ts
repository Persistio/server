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
  const queueDepthCheck = pool.query<{ depth: number }>(
    'SELECT COUNT(*)::int AS depth FROM extraction_queue WHERE claimed_at IS NULL'
  );
  let timeoutHandle: NodeJS.Timeout | undefined;

  void dbCheck.catch(() => undefined);
  void queueDepthCheck.catch(() => undefined);

  try {
    await Promise.race([
      dbCheck,
      new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => reject(new Error('Health check timed out')), HEALTH_DB_TIMEOUT_MS);
        timeoutHandle.unref();
      })
    ]);

    clearTimeout(timeoutHandle);
    let queueDepth: number | null = null;

    try {
      const result = await queueDepthCheck;
      queueDepth = result.rows[0]?.depth ?? 0;
    } catch {
      queueDepth = null;
    }

    return {
      db: 'ok',
      db_latency_ms: Date.now() - startedAt,
      queue_depth: queueDepth
    } as const;
  } catch {
    clearTimeout(timeoutHandle);
    return {
      db: 'degraded',
      db_latency_ms: Date.now() - startedAt,
      queue_depth: null
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
      queue_depth: database.queue_depth,
      uptime_s: Math.round(process.uptime())
    });
  });
}
