import { parentPort, workerData } from 'node:worker_threads';
import { installExecutionContext, assertExecutionLive, executionHeartbeat } from './context';
import { workerWakeSchema } from '../services/worker-wake-types';

interface Invocation { id: string; wake: unknown; deadline: number }
const invocation = workerData as Invocation;
const controller = new AbortController();
installExecutionContext(invocation.id, controller.signal, invocation.deadline);
const cancel = () => controller.abort(new Error('Request execution cancelled'));
parentPort?.on('message', message => {
  if (message?.type === 'cancel') cancel();
  if (message?.type === 'heartbeat') {
    try { executionHeartbeat(); } catch { cancel(); }
  }
});
const watchdog = setInterval(() => {
  try { assertExecutionLive(); } catch { cancel(); }
}, 250);
const deadline = setTimeout(cancel, Math.max(0, invocation.deadline - Date.now()));
// Independent hard fallback if the parent is stalled or an SDK ignores cancellation.
controller.signal.addEventListener('abort', () => setTimeout(() => process.exit(1), 2000), { once: true });

async function execute() {
  const wake = workerWakeSchema.parse(invocation.wake);
  const { getConfig } = await import('../config');
  const config = getConfig();
  config.EXTRACTION_BATCH_SIZE = 1;
  config.EXTRACTION_WORKER_CONCURRENCY = 1;
  config.CURATION_BATCH_SIZE = 1;
  const db = await import('../db/client');
  let closePublisher: (() => Promise<void>) | undefined;
  try {
    await db.prepareExistingDatabase();
    await (await import('./state')).initializeExecutionState(db.pool, invocation.id);
    if (config.ENCRYPTION_ENABLED) await (await import('../services/crypto')).initCryptoClient();
    assertExecutionLive();
    switch (wake.kind) {
      case 'extraction':
        await (await import('../daemon/extraction-execution')).createExtractionExecution().processBatch();
        break;
      case 'curation':
        if (!config.CURATOR_AUTO_RUN) throw new Error('Curation disabled');
        await (await import('../daemon/curation-execution')).createCurationExecution().processBatch();
        break;
      case 'contradiction': {
        if (!config.CONTRADICTION_SCAN_ENABLED) throw new Error('Contradiction activation disabled');
        const { ExtractorService } = await import('../services/extractor');
        const { admitContradictionArbitration } = await import('../services/worker-wake-store');
        await (await import('../services/contradiction-activation')).drainDueContradictionActivations(new ExtractorService(), {
          maxVaults: 1, admitArbitration: () => admitContradictionArbitration(db.pool, wake)
        });
        break;
      }
      case 'usage_periods':
        await (await import('../services/usage')).closeStaleUsagePeriods(1);
        break;
      case 'blob_reconciliation':
        await (await import('../services/raw-chunk-blob-reconciler')).reconcileStaleRawChunkBlobWrites(1);
        break;
      case 'platform_outbox': {
        if (config.EVENT_PUBLISHER === 'noop') throw new Error('Platform publisher disabled');
        const { createConfiguredEventPublisher } = await import('../events/event-publisher');
        const publisher = await createConfiguredEventPublisher(config);
        closePublisher = () => publisher.close?.() ?? Promise.resolve();
        const { EventOutboxDispatcher } = await import('../services/event-outbox-dispatcher');
        const result = await new EventOutboxDispatcher({
          db: db.pool, publisher, batchSize: 1, intervalMs: config.EVENT_OUTBOX_DISPATCH_INTERVAL_MS,
          maxAttempts: config.EVENT_OUTBOX_MAX_ATTEMPTS, maxRetryDelayMs: config.EVENT_OUTBOX_RETRY_MAX_DELAY_MS,
          retryBaseDelayMs: config.EVENT_OUTBOX_RETRY_BASE_DELAY_MS
        }).dispatchDueEvents();
        if (result.skipped) throw new Error('Platform publisher busy');
        break;
      }
    }
    assertExecutionLive();
    await (await import('./state')).checkpointExecutionState();
  } finally {
    try { await closePublisher?.(); }
    finally { await db.closePool(); }
  }
}

void execute().then(() => {
  assertExecutionLive();
  clearInterval(watchdog);
  clearTimeout(deadline);
  parentPort?.postMessage({ type: 'settled', success: true });
}, () => {
  clearInterval(watchdog);
  clearTimeout(deadline);
  parentPort?.postMessage({ type: 'settled', success: false });
}).catch(() => parentPort?.postMessage({ type: 'settled', success: false }));
// The host terminates this thread even after success, then checks tagged backends.
// An IPC result by itself never authorises wake acknowledgement.
