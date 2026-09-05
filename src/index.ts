import crypto from 'node:crypto';
import path from 'node:path';
import { Worker } from 'node:worker_threads';

import Fastify from 'fastify';
import type { FastifyReply } from 'fastify';

import { shutdownAzureMonitor } from './azure-monitor';
import { getConfig } from './config';
import { httpRequestDurationHistogram } from './http-metrics';
import type { JobRecord, JobStore } from './routes/jobs';
import { createConfiguredEventPublisher, shouldDispatchPlatformEvents } from './events/event-publisher';
import { registerPlatformOAuth } from './oauth';
import { registerAnalyticsRoutes } from './routes/analytics';
import { registerHealthRoutes } from './routes/health';
import { initCryptoClient } from './services/crypto';
import { recordCustomerApiRequestMetric } from './services/customer-api-request-metrics';
import { initCustomerMetrics, shutdownCustomerMetrics } from './services/customer-metrics';
import { getSpanAttributes } from './telemetry';

export type {
  JsonObject,
  JsonPrimitive,
  JsonValue,
  KnownPlatformEventType,
  PlatformEvent,
  PlatformEventPayload,
  PlatformEventPayloads,
  PlatformEventStatus,
  VaultUsagePeriodClosedPayload,
  VaultUsagePeriodLimitField,
  VaultUsagePeriodUsageField
} from './events/platform-event';

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

interface QuotaExceededLike extends Error {
  headers: Record<string, number | null>;
  statusCode: number;
}

function isQuotaExceededError(error: unknown): error is QuotaExceededLike {
  return error instanceof Error &&
    error.name === 'QuotaExceededError' &&
    typeof (error as { statusCode?: unknown }).statusCode === 'number' &&
    typeof (error as { headers?: unknown }).headers === 'object' &&
    (error as { headers?: unknown }).headers !== null;
}

function applyRateLimitHeaders(reply: FastifyReply, headers: Record<string, number | null>) {
  if (headers.limit !== null) {
    reply.header('X-RateLimit-Limit', headers.limit);
  }
  if (headers.remaining !== null) {
    reply.header('X-RateLimit-Remaining', headers.remaining);
  }
  if (headers.resetAtEpochSeconds !== null) {
    reply.header('X-RateLimit-Reset', headers.resetAtEpochSeconds);
  }
  if (headers.retryAfterSeconds !== null) {
    reply.header('Retry-After', headers.retryAfterSeconds);
  }
}

async function main() {
  const config = getConfig();
  const isAnalyticsApi = config.PERSISTIO_MODE === 'analytics-api';
  const shouldStartWorker = config.PERSISTIO_MODE === 'combined' || config.PERSISTIO_MODE === 'worker';
  const shouldStartCurationWorker = shouldStartWorker && config.CURATOR_AUTO_RUN;
  const shouldRegisterFullApi = config.PERSISTIO_MODE === 'combined' || config.PERSISTIO_MODE === 'api';

  let closeDbPool: (() => Promise<void>) | undefined;

  if (config.ENCRYPTION_ENABLED && !isAnalyticsApi) {
    await initCryptoClient();
  }

  if (shouldRegisterFullApi) {
    const db = await import('./db/client');
    closeDbPool = db.closePool;
    const { runMigrations } = db;
    await runMigrations();
  }

  const app = Fastify({
    logger: {
      level: process.env.LOG_LEVEL ?? 'info',
      mixin() {
        return getSpanAttributes({});
      }
    }
  });
  const eventPublisher = shouldStartWorker
    ? await createConfiguredEventPublisher(config, app.log)
    : undefined;
  await initCustomerMetrics(config, app.log);
  const shouldStartEventOutboxDispatcher = shouldStartWorker && shouldDispatchPlatformEvents(config);
  app.log.info({
    event_outbox_dispatcher_enabled: shouldStartEventOutboxDispatcher,
    event_publisher: config.EVENT_PUBLISHER,
    persistio_mode: config.PERSISTIO_MODE
  }, 'Event publisher configured');

  app.addHook('onResponse', async (request, reply) => {
    const route = request.routeOptions.url ?? request.url;
    httpRequestDurationHistogram.record(reply.elapsedTime, {
      method: request.method,
      route,
      status_code: String(reply.statusCode)
    });
    recordCustomerApiRequestMetric(request, reply);
  });

  app.setErrorHandler((error, _request, reply) => {
    if (isQuotaExceededError(error)) {
      applyRateLimitHeaders(reply, error.headers);
      reply.code(error.statusCode).send({ error: error.message });
      return;
    }

    reply.send(error);
  });

  await registerPlatformOAuth(app, config);

  const jobs = new InMemoryJobStore();
  const worker = shouldStartWorker
    ? new Worker(path.resolve(__dirname, 'daemon', 'extraction-worker.js'), {
      execArgv: ['--require', path.resolve(__dirname, 'preload.js')]
    })
    : undefined;
  const curationWorker = shouldStartCurationWorker
    ? new Worker(path.resolve(__dirname, 'daemon', 'curation-worker.js'), {
      execArgv: ['--require', path.resolve(__dirname, 'preload.js')]
    })
    : undefined;

  let startedEventOutboxDispatcher: { start(): void; stop(): Promise<void> } | undefined;
  let startedUsagePeriodSweeper: { start(): void; stop(): Promise<void> } | undefined;
  if (shouldStartEventOutboxDispatcher && eventPublisher) {
    const db = await import('./db/client');
    closeDbPool ??= db.closePool;
    const { EventOutboxDispatcher } = await import('./services/event-outbox-dispatcher');
    startedEventOutboxDispatcher = new EventOutboxDispatcher({
      batchSize: config.EVENT_OUTBOX_BATCH_SIZE,
      intervalMs: config.EVENT_OUTBOX_DISPATCH_INTERVAL_MS,
      logger: app.log,
      maxAttempts: config.EVENT_OUTBOX_MAX_ATTEMPTS,
      maxRetryDelayMs: config.EVENT_OUTBOX_RETRY_MAX_DELAY_MS,
      publisher: eventPublisher,
      publisherName: config.EVENT_PUBLISHER,
      retryBaseDelayMs: config.EVENT_OUTBOX_RETRY_BASE_DELAY_MS,
      warnDepthThreshold: config.EVENT_OUTBOX_WARN_DEPTH_THRESHOLD,
      warnOldestAgeMs: config.EVENT_OUTBOX_WARN_OLDEST_AGE_MS
    });
  }

  if (shouldStartWorker) {
    const db = await import('./db/client');
    closeDbPool ??= db.closePool;
    const { UsagePeriodSweeper } = await import('./services/usage-period-sweeper');
    startedUsagePeriodSweeper = new UsagePeriodSweeper({
      batchSize: config.USAGE_PERIOD_SWEEP_BATCH_SIZE,
      intervalMs: config.USAGE_PERIOD_SWEEP_INTERVAL_MS,
      logger: app.log
    });
  }

  startedEventOutboxDispatcher?.start();
  startedUsagePeriodSweeper?.start();

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

  curationWorker?.on('error', (error) => {
    app.log.error(error, 'Curation worker failed');
  });

  curationWorker?.on('exit', (code) => {
    if (code !== 0) {
      app.log.error({ code }, 'Curation worker exited unexpectedly');
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

  const shutdownWorker = async (target: Worker | undefined, name: string) => {
    if (!target) return;

    await new Promise<void>((resolve) => {
      let settled = false;
      let timeout: NodeJS.Timeout | undefined;
      const finish = () => {
        if (settled) return;
        settled = true;
        if (timeout) clearTimeout(timeout);
        target.off('message', onMessage);
        target.off('exit', onExit);
        resolve();
      };
      const terminate = () => {
        app.log.warn({ worker: name }, 'Worker did not acknowledge shutdown; terminating');
        void target.terminate().finally(finish);
      };
      const onMessage = (message: unknown) => {
        if (typeof message === 'object' && message !== null && (message as { type?: string }).type === 'shutdown-complete') {
          finish();
        }
      };
      const onExit = () => {
        finish();
      };

      timeout = setTimeout(terminate, 10_000);
      target.on('message', onMessage);
      target.once('exit', onExit);

      try {
        target.postMessage({ type: 'shutdown' });
      } catch (error) {
        app.log.warn({ err: error, worker: name }, 'Worker shutdown message failed; terminating');
        terminate();
      }
    });
  };

  await registerHealthRoutes(app, config);
  if (isAnalyticsApi) {
    await registerAnalyticsRoutes(app, config);
  }
  if (shouldRegisterFullApi) {
    const [
      { registerIngestRoutes },
      { registerRecallRoutes },
      { registerMemoryRoutes },
      { registerJobRoutes },
      { registerStatsRoutes },
      { registerCurationRoutes },
      { registerAdminRoutes }
    ] = await Promise.all([
      import('./routes/ingest'),
      import('./routes/recall'),
      import('./routes/memories'),
      import('./routes/jobs'),
      import('./routes/stats'),
      import('./routes/curation'),
      import('./routes/admin')
    ]);

    await registerIngestRoutes(app, worker ? triggerExtraction : undefined);
    await registerRecallRoutes(app);
    await registerMemoryRoutes(app);
    await registerJobRoutes(app, jobs, triggerExtraction);
    await registerStatsRoutes(app);
    await registerCurationRoutes(app);
    await registerAdminRoutes(app);
  }

  const shutdown = async () => {
    await startedUsagePeriodSweeper?.stop();
    await startedEventOutboxDispatcher?.stop();
    await eventPublisher?.close?.();
    await app.close();
    await shutdownWorker(curationWorker, 'curation');
    await shutdownWorker(worker, 'extraction');
    await shutdownCustomerMetrics();
    await closeDbPool?.();
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
