import { parentPort } from 'node:worker_threads';
import { createOperationalLogger } from '../operational-metadata';
import { getConfig } from '../config';
import { closePool } from '../db/client';
import { initCryptoClient } from '../services/crypto';
import { initCustomerMetrics, shutdownCustomerMetrics } from '../services/customer-metrics';
import { shutdownTelemetry } from '../azure-monitor';
import { createShutdownDeadline, drainRuntimeOwner, parseShutdownDeadline } from '../runtime-shutdown';
import { getSpanAttributes } from '../telemetry';
import { createExtractionExecution } from './extraction-execution';
import { extractionJobsCounter } from '../metrics';

const operationalLog = createOperationalLogger('extraction-worker');
interface WorkerRunOnceRequest {
  type: 'run-once';
  jobId?: string;
  vaultId?: string;
}

interface WorkerShutdownRequest {
  type: 'shutdown';
  deadline?: unknown;
}

type WorkerRequest = WorkerRunOnceRequest | WorkerShutdownRequest;

const config = getConfig();
const execution = createExtractionExecution();

// Compatibility export; execution ownership remains stable for this worker.
export const processBatch = execution.processBatch;

const activeWorkerTasks = new Set<Promise<unknown>>();
let isShuttingDown = false;
let resolveWorkerSleep: (() => void) | null = null;
let shutdownPromise: Promise<void> | null = null;
let workerLoop: Promise<void> | undefined;
let workerLoopFailed = false;

function trackWorkerTask<T>(task: Promise<T>): Promise<T> {
  const tracked = task.finally(() => {
    activeWorkerTasks.delete(tracked);
  });
  activeWorkerTasks.add(tracked);
  return tracked;
}

function sleepUntilNextBatch(ms: number): Promise<void> {
  if (isShuttingDown) return Promise.resolve();

  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      resolveWorkerSleep = null;
      resolve();
    }, ms);
    resolveWorkerSleep = () => {
      clearTimeout(timeout);
      resolveWorkerSleep = null;
      resolve();
    };
  });
}

async function shutdownWorker(deadlineValue?: unknown) {
  if (!shutdownPromise) {
    shutdownPromise = (async () => {
      isShuttingDown = true;
      resolveWorkerSleep?.();
      let complete = false;
      try {
        const deadline = parseShutdownDeadline(deadlineValue) ?? createShutdownDeadline(Boolean(process.env.K_SERVICE));
        await drainRuntimeOwner({
          // Task rejection is not drain failure: the loop/handler owns business
          // errors. All known tasks must settle before resource cleanup.
          drain: [async () => { await Promise.allSettled([workerLoop, ...Array.from(activeWorkerTasks)]); }],
          publishers: [shutdownCustomerMetrics],
          telemetry: shutdownTelemetry,
          pool: closePool,
          deadline
        });
        complete = !workerLoopFailed;
      } catch {
        operationalLog.warn('[persistio] Worker shutdown incomplete');
      } finally {
        try { parentPort?.postMessage({ type: complete ? 'shutdown-complete' : 'shutdown-failed' }); }
        finally { parentPort?.close(); }
      }
    })();
  }

  await shutdownPromise;
}

async function runLoop() {
  if (config.ENCRYPTION_ENABLED) {
    await initCryptoClient();
  }
  await initCustomerMetrics(config);

  while (!isShuttingDown) {
    try {
      await trackWorkerTask(processBatch());
    } catch (error) {
      if (!isShuttingDown) {
        operationalLog.error('Extraction loop iteration failed', error);
      }
    }

    if (!isShuttingDown) {
      try {
        await trackWorkerTask(execution.activateDueContradictions());
      } catch (error) {
        operationalLog.error('Contradiction activation iteration failed', error);
      }
    }

    await sleepUntilNextBatch(config.EXTRACTION_INTERVAL_MS);
  }
}

export async function handleRunOnce(message: WorkerRunOnceRequest) {
  if (!parentPort) {
    return;
  }

  if (isShuttingDown) {
    if (message.jobId) {
      parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'failed', error: 'Extraction worker is shutting down' });
    }
    return;
  }

  if (message.jobId) {
    parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'running' });
  }

  try {
    await trackWorkerTask(processBatch(message.vaultId));
    extractionJobsCounter.add(1, {
      status: 'success',
      vault_id: message.vaultId ?? 'all'
    });
    if (message.jobId) {
      parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'completed' });
    }
  } catch (error) {
    extractionJobsCounter.add(1, {
      status: 'error',
      vault_id: message.vaultId ?? 'all'
    });
    if (message.jobId) {
      parentPort.postMessage({
        type: 'job-status',
        jobId: message.jobId,
        status: 'failed',
        error: error instanceof Error ? error.message : 'Unknown worker error'
      });
    }
  }
}

if (parentPort) {
  parentPort.on('message', (message: WorkerRequest) => {
    if (message.type === 'run-once') {
      // Include status/counter completion after processBatch in the drain barrier.
      if (!isShuttingDown) void trackWorkerTask(handleRunOnce(message)).catch(() => process.exit(1));
      else void handleRunOnce(message).catch(() => process.exit(1));
    } else if (message.type === 'shutdown') {
      void shutdownWorker(message.deadline).catch(() => process.exit(1));
    }
  });
}

if (parentPort) {
  workerLoop = runLoop();
  // The failure handler is outside the promise shutdown waits for.
  void workerLoop.catch(async (error) => {
    workerLoopFailed = true;
    try { operationalLog.error(getSpanAttributes({ error }), 'Extraction worker terminated'); }
    finally { try { await shutdownWorker(); } finally { process.exit(1); } }
  });
}
