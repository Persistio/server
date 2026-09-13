import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('worker-effects');
import { recordCommittedApiQuotaReservation, recordMemoryCountDelta, type ApiQuotaReservation } from './usage';
import type { CustomerMetricSource } from './customer-metrics';

export type WorkerEffect = { kind: 'quota'; reservation: ApiQuotaReservation } | {
  kind: 'memory-count'; vaultId: string; accountId: string | null; delta: number; source: CustomerMetricSource;
};

/** Data only, owned by one outer transaction attempt. Never publish on rollback
 * or unknown COMMIT. Delivery is best-effort, not a durable event outbox. */
export function publishCommittedWorkerEffects(effects: readonly WorkerEffect[]): void {
  for (const effect of effects) {
    try {
      if (effect.kind === 'quota') recordCommittedApiQuotaReservation(effect.reservation);
      else recordMemoryCountDelta(effect.vaultId, effect.accountId, effect.delta, effect.source);
    } catch {
      try { operationalLog.warn('Committed worker metric publication failed; business result retained'); }
      catch { /* Diagnostics must not re-enter business retry handling either. */ }
    }
  }
}
