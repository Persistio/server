import { parentPort } from 'node:worker_threads';
import { createOperationalLogger } from '../operational-metadata';
import { getConfig } from '../config';
import { closePool } from '../db/client';
import { initCryptoClient } from '../services/crypto';
import { initCustomerMetrics, shutdownCustomerMetrics } from '../services/customer-metrics';
import { shutdownTelemetry } from '../azure-monitor';
import { createShutdownDeadline, drainRuntimeOwner, parseShutdownDeadline } from '../runtime-shutdown';
import { getSpanAttributes } from '../telemetry';
import { createCurationExecution } from './curation-execution';

const operationalLog = createOperationalLogger('curation-worker');
interface WorkerShutdownRequest {
  type: 'shutdown';
  deadline?: unknown;
}

const config = getConfig();
const execution = createCurationExecution();

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
        operationalLog.error('Curation loop iteration failed', error);
      }
    }

    await sleepUntilNextBatch(config.CURATION_INTERVAL_MS);
  }
}

if (parentPort) {
  parentPort.on('message', (message: WorkerShutdownRequest) => {
    if (message.type === 'shutdown') {
      void shutdownWorker(message.deadline).catch(() => process.exit(1));
    }
  });
}

if (parentPort) {
  workerLoop = runLoop();
  // The failure handler is outside the promise shutdown waits for.
  void workerLoop.catch(async (error) => {
    workerLoopFailed = true;
    try { operationalLog.error(getSpanAttributes({ error }), 'Curation worker terminated'); }
    finally { try { await shutdownWorker(); } finally { process.exit(1); } }
  });
}
