import type { Pool } from 'pg';
import { z } from 'zod';
import { getConfig } from '../config';
import { CloudTasksWakeTransport } from './worker-wake-cloud-tasks';
import { WorkerWakeDispatcher } from './worker-wake-dispatcher';
import { WorkerWakeReconciliation } from './worker-wake-reconciliation';
import { WORKER_WAKE_KINDS } from './worker-wake-types';
import { WorkerWakeStore } from './worker-wake-store';

/** Explicit opt-in. The observer supplies enablement flags without receiving AI secrets. */
export function configuredWakeDispatcher(pool: Pick<Pool, 'connect'>, flags: {
  curationEnabled: boolean; contradictionEnabled: boolean;
}, env: NodeJS.ProcessEnv = process.env): WorkerWakeDispatcher | undefined {
  const enabled = z.enum(['true', 'false']).default('false').parse(env.WORKER_DISPATCH_ENABLED) === 'true';
  if (!enabled) return;
  const config = getConfig();
  return new WorkerWakeDispatcher({ enabled: true,
    kinds: WORKER_WAKE_KINDS.filter(kind => (kind !== 'curation' || flags.curationEnabled) && (kind !== 'contradiction' || flags.contradictionEnabled)),
    store: new WorkerWakeStore(pool),
    reconciliation: new WorkerWakeReconciliation(pool, flags),
    transport: new CloudTasksWakeTransport({ enabled: true, queue: env.WORKER_TASK_QUEUE,
      workerOrigin: env.WORKER_TASK_ORIGIN, invokerEmail: env.WORKER_TASK_INVOKER_EMAIL,
      dispatchDeadlineSeconds: 150 }),
    contradictionAllowance: config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH,
    contradictionCycleMs: config.EXTRACTION_INTERVAL_MS });
}
