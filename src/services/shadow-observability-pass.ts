import { createOperationalLogger } from '../operational-metadata';
import type { ShadowDeliveryReport } from './customer-metric-shadow';
import type { WorkerObservation } from './shadow-observability';

const logger = createOperationalLogger('shadow-observability');
export interface ShadowDeliveryObservation {
  event: 'shadow_customer_delivery';
  shadow_kind: 'customer_metrics';
  shadow_outcome: 'completed' | 'failed';
  shadow_health: 'ok' | 'monitor_error';
  duration_ms: number;
  delivery_delivered?: number;
  delivery_retried?: number;
  delivery_stale?: number;
  delivery_dead?: number;
  delivery_purged?: number;
}

/** A delivery failure must remain visible even when the following SQL scan works.
 * Transport exceptions can have unknown partial progress: do not fabricate counts
 * or export exception text. Resource cleanup and hard deadline belong to the host. */
export async function runShadowObservabilityPass(options: {
  dispatch?: () => Promise<ShadowDeliveryReport>;
  observe: () => Promise<WorkerObservation[]>;
  emit?: (record: ShadowDeliveryObservation) => void;
}): Promise<0 | 1> {
  let exitCode: 0 | 1 = 0;
  if (options.dispatch) {
    const started = Date.now();
    let observation: ShadowDeliveryObservation;
    const base = {event:'shadow_customer_delivery', shadow_kind:'customer_metrics'} as const;
    try {
      const report = await options.dispatch();
      const failed = report.retried > 0 || report.stale > 0 || report.dead > 0;
      if (failed) exitCode = 1;
      observation = {...base, shadow_outcome:failed ? 'failed' : 'completed', shadow_health:'ok',
        duration_ms:Math.max(0,Date.now()-started), delivery_delivered:report.delivered,
        delivery_retried:report.retried, delivery_stale:report.stale,
        delivery_dead:report.dead, delivery_purged:report.purged};
    } catch {
      exitCode = 1;
      observation = {...base, shadow_outcome:'failed', shadow_health:'monitor_error', duration_ms:Math.max(0,Date.now()-started)};
    }
    try { (options.emit ?? (record => logger.info(record)))(observation); }
    catch { exitCode = 1; }
  }
  try {
    if ((await options.observe()).some(item => item.shadow_health === 'monitor_error')) exitCode = 1;
  } catch { exitCode = 1; }
  return exitCode;
}
