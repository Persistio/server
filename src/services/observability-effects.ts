import * as metrics from '../metrics';
import type { CounterLike, HistogramLike } from '../telemetry';

// Diagnostics must not replace a committed result or the original policy error.
export function emitObservability(effect: () => void): void {
  try { effect(); } catch { /* durable SQL evidence remains authoritative */ }
}
function counter(instrument: () => CounterLike): CounterLike {
  return { add: (value, attributes) => emitObservability(() => instrument().add(value, attributes)) };
}
export const memoryPolicyEventCounter = counter(() => metrics.memoryPolicyEventCounter);
export const recallDeliveryCounter = counter(() => metrics.recallDeliveryCounter);
export const recallDeliveryMissingAckCounter = counter(() => metrics.recallDeliveryMissingAckCounter);
export const globalRuleDeliveryCounter = counter(() => metrics.globalRuleDeliveryCounter);
export const recallDurationHistogram: HistogramLike = {
  record: (value, attributes) => emitObservability(() => metrics.recallDurationHistogram.record(value, attributes))
};
