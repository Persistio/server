import { shadowDispatchCycle } from './shadow-observability';
import { WORKER_WAKE_KINDS, type WakeTransport, type WorkerWakeKind } from './worker-wake-types';
import { WorkerWakeStore, type WakeReservation } from './worker-wake-store';

export type WakeDispatchOutcome = 'idle' | 'created' | 'retained' | 'rotated' | 'stale' | 'failed';
export interface WakeDispatchReport {
  enabled: boolean;
  outcomes: Partial<Record<WorkerWakeKind, WakeDispatchOutcome>>;
  failures: Partial<Record<WorkerWakeKind, 'reconciliation' | 'storage' | 'transport'>>;
}
export interface WorkerWakeDispatcherOptions {
  enabled?: boolean;
  store: WorkerWakeStore;
  reconciliation: { dueAt(kind: WorkerWakeKind): Promise<Date | null> };
  transport: WakeTransport;
  contradictionAllowance: number;
  contradictionCycleMs: number;
}

/** One explicit pass, no timer/startup or business execution. A future scheduled
 * request host must await this pass and expose failures independently of scan success. */
export class WorkerWakeDispatcher {
  constructor(private readonly options: WorkerWakeDispatcherOptions) {}

  async dispatchOnce(): Promise<WakeDispatchReport> {
    if (!this.options.enabled) return { enabled: false, outcomes: {}, failures: {} };
    const { store, reconciliation, transport } = this.options;
    const report: WakeDispatchReport = { enabled: true, outcomes: {}, failures: {} };
    for (const kind of WORKER_WAKE_KINDS) {
      let reservation: WakeReservation | null = null;
      let stage: 'reconciliation' | 'storage' | 'transport' = 'reconciliation';
      try {
        const dueAt = await reconciliation.dueAt(kind);
        stage = 'storage';
        if (dueAt) await store.reconcile({ kind, dueAt });
        reservation = await store.reserve(kind, this.options.contradictionAllowance, this.options.contradictionCycleMs);
        if (!reservation) { report.outcomes[kind] = 'idle'; continue; }
        stage = 'transport';
        if (reservation.dispatched) {
          const exists = await transport.exists(reservation.wake);
          stage = 'storage';
          const updated = exists ? await store.dispatched(reservation) : await store.rotate(reservation);
          report.outcomes[kind] = updated ? (exists ? 'retained' : 'rotated') : 'stale';
        } else {
          const result = await transport.create(reservation.wake, reservation.dueAt);
          // A retained task ID can be a tombstone; it is not a completion receipt.
          if (result === 'exists' && !await transport.exists(reservation.wake)) {
            stage = 'storage';
            report.outcomes[kind] = await store.rotate(reservation) ? 'rotated' : 'stale';
          } else {
            stage = 'storage';
            report.outcomes[kind] = await store.dispatched(reservation) ? (result === 'created' ? 'created' : 'retained') : 'stale';
          }
        }
      } catch {
        report.outcomes[kind] = 'failed';
        report.failures[kind] = stage;
        // If settlement itself is uncertain, the durable lease expires for repair.
        // Never turn this into a business retry or expose provider/SQL error contents.
        if (reservation) await store.failed(reservation).catch(() => {});
      }
    }
    for (const kind of WORKER_WAKE_KINDS) shadowDispatchCycle(kind,report.outcomes[kind] ?? 'unknown');
    return report;
  }
}
