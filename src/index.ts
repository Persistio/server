import crypto from 'node:crypto';
import path from 'node:path';
import { Worker } from 'node:worker_threads';

import Fastify from 'fastify';
import { shutdownAzureMonitor } from '@azure/monitor-opentelemetry';

import { getConfig } from './config';
import { closePool, runMigrations } from './db/client';
import { httpRequestDurationHistogram } from './metrics';
import type { JobRecord, JobStore } from './routes/jobs';
import { registerAdminRoutes } from './routes/admin';
import { registerHealthRoutes } from './routes/health';
import { registerIngestRoutes } from './routes/ingest';
import { registerJobRoutes } from './routes/jobs';
import { registerMemoryRoutes } from './routes/memories';
import { registerRecallRoutes } from './routes/recall';
import { registerStatsRoutes } from './routes/stats';
import { initCryptoClient } from './services/crypto';
import { getSpanAttributes } from './telemetry';

class InMemoryJobStore implements JobStore {
  private readonly jobs = new Map<string, JobRecord>();

  create(vaultId: string): JobRecord {
    const timestamp = new Date().toISOString();
    const job: JobRecord = {
      id: crypto.randomUUID(),
      vaultId,
      status: 'queued',
      createdAt: timestamp,
      updatedAt: timestamp
    };
    this.jobs.set(job.id, job);
    return job;
  }

  get(jobId: string) {
    return this.jobs.get(jobId);
  }

  update(jobId: string, status: JobRecord['status'], error?: string) {
    const current = this.jobs.get(jobId);
    if (!current) {
      return undefined;
    }

    const next: JobRecord = {
      ...current,
      status,
      updatedAt: new Date().toISOString(),
      error
    };
    this.jobs.set(jobId, next);
    return next;
  }
}

async function main() {
  const config = getConfig();
  const isApiMode = config.PERSISTIO_MODE === 'api';
  const isWorkerMode = config.PERSISTIO_MODE === 'worker';
  const shouldStartWorker = config.PERSISTIO_MODE !== 'api';
  const shouldRegisterFullApi = config.PERSISTIO_MODE !== 'worker';

  if (config.ENCRYPTION_ENABLED) {
    await initCryptoClient();
  }

  await runMigrations();

  const app = Fastify({
    logger: {
      level: process.env.LOG_LEVEL ?? 'info',
      mixin() {
        return getSpanAttributes({});
      }
    }
  });

  app.addHook('onResponse', async (request, reply) => {
    const route = request.routeOptions.url ?? request.url;
    httpRequestDurationHistogram.record(reply.elapsedTime, {
      method: request.method,
      route,
      status_code: String(reply.statusCode)
    });
  });

  const jobs = new InMemoryJobStore();
  const worker = shouldStartWorker
    ? new Worker(path.resolve(__dirname, 'daemon', 'extraction-worker.js'), {
      execArgv: ['--require', path.resolve(__dirname, 'preload.js')]
    })
    : undefined;

  worker?.on('message', (message: { type: string; jobId?: string; status?: JobRecord['status']; error?: string }) => {
    if (message.type === 'job-status' && message.jobId && message.status) {
      jobs.update(message.jobId, message.status, message.error);
    }
  });

  worker?.on('error', (error) => {
    app.log.error(error, 'Extraction worker failed');
  });

  worker?.on('exit', (code) => {
    if (code !== 0) {
      app.log.error({ code }, 'Extraction worker exited unexpectedly');
    }
  });

  const triggerExtraction = (jobId: string, vaultId?: string) => {
    if (!worker) {
      jobs.update(jobId, 'failed', 'Extraction worker is not running in API-only mode');
      return;
    }

    worker.postMessage({
      type: 'run-once',
      jobId,
      vaultId
    });
  };

  await registerHealthRoutes(app, config);
  if (shouldRegisterFullApi) {
    await registerIngestRoutes(app);
    await registerRecallRoutes(app);
    await registerMemoryRoutes(app);
    await registerJobRoutes(app, jobs, triggerExtraction);
    await registerStatsRoutes(app);
    await registerAdminRoutes(app);
  }

  const shutdown = async () => {
    await app.close();
    await closePool();
    await worker?.terminate();
    await shutdownAzureMonitor();
  };

  process.on('SIGINT', () => {
    void shutdown().finally(() => process.exit(0));
  });
  process.on('SIGTERM', () => {
    void shutdown().finally(() => process.exit(0));
  });

  await app.listen({
    port: config.PORT,
    host: '0.0.0.0'
  });
}

void main().catch((error) => {
  console.error(error);
  process.exit(1);
});
