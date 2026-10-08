import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('worker-effects');
import { buildQuotaDeltaEvent, recordCommittedApiQuotaReservation, type ApiQuotaReservation } from './usage';
import { recordCustomerMetric, type CustomerMetricEvent, type CustomerMetricSource } from './customer-metrics';
import { captureShadowCustomerMetric } from './customer-metric-shadow';

export type WorkerEffect = { kind: 'quota'; reservation: ApiQuotaReservation } | {
  kind: 'memory-count'; event: CustomerMetricEvent | null;
};

export function memoryCountEffect(vaultId: string, accountId: string | null, delta: number, source: CustomerMetricSource): WorkerEffect {
  return {kind:'memory-count', event:buildQuotaDeltaEvent({vaultId,accountId,delta,source,operation:'memory_count'})};
}

/** Data only, owned by one outer transaction attempt. Never publish on rollback
 * or unknown COMMIT. This legacy publisher remains best effort; the outer SQL
 * transaction separately captures shadow intents before committing. */
export function publishCommittedWorkerEffects(effects: readonly WorkerEffect[]): void {
  for (const effect of effects) {
    try {
      if (effect.kind === 'quota') recordCommittedApiQuotaReservation(effect.reservation);
      else if (effect.event) recordCustomerMetric(effect.event);
    } catch {
      try { operationalLog.warn('Committed worker metric publication failed; business result retained'); }
      catch { /* Diagnostics must not re-enter business retry handling either. */ }
    }
  }
}

/** Quota reservations capture themselves. Persist only capacity effects here, once
 * at the outer transaction boundary, before releasing ownership or committing. */
export async function captureShadowWorkerEffects(client: import('pg').PoolClient, effects: readonly WorkerEffect[]): Promise<void> {
  for (const effect of effects) if (effect.kind === 'memory-count' && effect.event) {
    await captureShadowCustomerMetric(client, effect.event);
  }
}
