import { withTransaction } from '../db/client';
import { meter, type ObservableResultLike } from '../telemetry';

export interface DeliveryHealth { overdue: number; integrity_error: number }
export const DELIVERY_HEALTH_SQL = `WITH cutoff AS MATERIALIZED (
  SELECT clock_timestamp() - interval '5 minutes' AS before
) SELECT
  (SELECT count(*)::int FROM memory_delivery_pending WHERE NOT integrity_error AND created_at < (SELECT before FROM cutoff)) AS overdue,
  (SELECT count(*)::int FROM memory_delivery_pending WHERE integrity_error) AS integrity_error`;

export async function readDeliveryHealth(): Promise<DeliveryHealth> {
  return withTransaction(async client => {
    await client.query('SET TRANSACTION READ ONLY');
    await client.query("SET LOCAL statement_timeout = '2s'");
    await client.query("SET LOCAL lock_timeout = '1s'");
    const result = await client.query<DeliveryHealth>(DELIVERY_HEALTH_SQL);
    return result.rows[0];
  });
}

export function createDeliveryHealthCollector(read = readDeliveryHealth) {
  let active: Promise<DeliveryHealth> | undefined;
  return async (result: ObservableResultLike): Promise<void> => {
    // Coalesce overlapping SDK collection, not successes from previous periods.
    const pending = active ??= Promise.resolve().then(read);
    const observe = (value: number, state: string) => {
      try { result.observe(value, { state }); } catch { /* exporter owns its errors */ }
    };
    try {
      const health = await pending;
      observe(health.overdue, 'overdue');
      observe(health.integrity_error, 'integrity_error');
      observe(0, 'monitor_error');
    } catch {
      // No healthy-zero fallback. The absence policy independently checks export.
      observe(1, 'monitor_error');
    } finally { if (active === pending) active = undefined; }
  };
}

let registered = false;
export function registerDeliveryHealthMetrics(): void {
  if (registered) return;
  registered = true;
  meter.createObservableGauge('persistio.recall.delivery_health', {
    description: 'Current unacknowledged delivery health; fixed states, database-wide replica snapshot'
  }).addCallback(createDeliveryHealthCollector());
}
