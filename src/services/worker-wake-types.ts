import { z } from 'zod';

export const WORKER_WAKE_KINDS = [
  'extraction', 'curation', 'contradiction', 'platform_outbox', 'usage_periods', 'blob_reconciliation'
] as const;
export type WorkerWakeKind = typeof WORKER_WAKE_KINDS[number];

export const workerWakeSchema = z.object({
  version: z.literal(1),
  kind: z.enum(WORKER_WAKE_KINDS),
  token: z.string().uuid(),
  generation: z.string().regex(/^[1-9][0-9]{0,18}$/).refine(value => BigInt(value) <= 9223372036854775807n)
}).strict();
export type WorkerWake = z.infer<typeof workerWakeSchema>;

export interface WakeTransport {
  create(wake: WorkerWake, dueAt: Date): Promise<'created' | 'exists'>;
  /** False requires confirmed task absence AND a readable, existing queue. */
  exists(wake: WorkerWake): Promise<boolean>;
}
