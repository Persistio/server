import { createOperationalLogger } from './operational-metadata';
import { operationalLoggerOptions } from './operational-metadata';
import { registerPlatformErrorHandler } from './http-error-handler';
const operationalLog=createOperationalLogger('index');
import crypto from 'node:crypto';
import path from 'node:path';
import { Worker } from 'node:worker_threads';

import Fastify from 'fastify';

import { shutdownAzureMonitor } from './azure-monitor';
import { closeRuntimeHttp, createRuntimeShutdown, drainRuntimeOwner, stopRuntimeWorker } from './runtime-shutdown';
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

  if (!isAnalyticsApi) {
    const db = await import('./db/client');
    closeDbPool = db.closePool;
    const { runMigrations } = db;
    await runMigrations();
  }

  const app = Fastify({
    disableRequestLogging:true,
    logger: operationalLoggerOptions(()=>getSpanAttributes({}))
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
    const route = request.routeOptions.url ?? '/unmatched';
    httpRequestDurationHistogram.record(reply.elapsedTime, {
      method: request.method,
      route,
      status_code: String(reply.statusCode)
    });
    recordCustomerApiRequestMetric(request, reply);
  });

  registerPlatformErrorHandler(app);

  await registerPlatformOAuth(app, config);

  const jobs = new InMemoryJobStore();
  const worker = shouldStartWorker
    ? new Worker(path.resolve(__dirname, 'daemon', 'extraction-worker.js'), {
      execArgv: ['--require', path.resolve(__dirname, 'preload.js')],
      workerData: { component: 'extraction' }
    })
    : undefined;
  const curationWorker = shouldStartCurationWorker
    ? new Worker(path.resolve(__dirname, 'daemon', 'curation-worker.js'), {
      execArgv: ['--require', path.resolve(__dirname, 'preload.js')],
      workerData: { component: 'curation' }
    })
    : undefined;

  let startedEventOutboxDispatcher: { start(): void; stop(): Promise<void> } | undefined;
  let startedUsagePeriodSweeper: { start(): void; stop(): Promise<void> } | undefined;
  let startedRawChunkBlobReconciler: { start(): void; stop(): Promise<void> } | undefined;
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
    const { RawChunkBlobReconciler } = await import('./services/raw-chunk-blob-reconciler');
    startedRawChunkBlobReconciler = new RawChunkBlobReconciler({
      batchSize: config.RAW_CHUNK_RECONCILE_BATCH_SIZE,
      intervalMs: config.RAW_CHUNK_RECONCILE_INTERVAL_MS,
      logger: app.log
    });
  }

  startedEventOutboxDispatcher?.start();
  startedUsagePeriodSweeper?.start();
  startedRawChunkBlobReconciler?.start();

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

  let httpDrain: Promise<void> | undefined;
  const stopHttp = () => httpDrain ??= closeRuntimeHttp(() => app.close(), () => app.server.closeIdleConnections());
  const shutdown = createRuntimeShutdown({
    cloudRun: Boolean(process.env.K_SERVICE),
    warn: message => app.log.warn(message),
    exit: code => process.exit(code),
    branches: [
      deadline => drainRuntimeOwner({
        drain: [stopHttp, () => startedUsagePeriodSweeper?.stop(),
          () => startedRawChunkBlobReconciler?.stop(), () => startedEventOutboxDispatcher?.stop()],
        publishers: [() => eventPublisher?.close?.(), shutdownCustomerMetrics],
        telemetry: shutdownAzureMonitor,
        pool: () => closeDbPool?.(),
        deadline
      }),
      deadline => stopRuntimeWorker(curationWorker, deadline),
      async deadline => {
        // An admitted combined-mode request can still dispatch extraction work.
        if (config.PERSISTIO_MODE === 'combined') await stopHttp();
        await stopRuntimeWorker(worker, deadline);
      }
    ]
  });

  process.on('SIGINT', () => {
    void shutdown();
  });
  process.on('SIGTERM', () => {
    void shutdown();
  });

  await app.listen({
    port: config.PORT,
    host: '0.0.0.0'
  });
}

void main().catch((error) => {
  operationalLog.error(error);
  process.exit(1);
});
