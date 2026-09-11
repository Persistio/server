import { withTransaction } from '../db/client';
import { getRawChunkStorage, type RawChunkStorage } from './raw-chunk-storage';
import { cleanupRawChunkWrite } from './raw-chunk-write-lifecycle';

const DEFAULT_STALE_AFTER_MS = 15 * 60 * 1000;

export interface RawChunkBlobReconcileResult {
  selected: number;
  committed: number;
  deleted: number;
  failed: number;
  quarantined?: number;
}

export async function reconcileStaleRawChunkBlobWrites(
  batchSize: number,
  staleAfterMs = DEFAULT_STALE_AFTER_MS,
  storage: RawChunkStorage = getRawChunkStorage(),
  transaction: typeof withTransaction = withTransaction
): Promise<RawChunkBlobReconcileResult> {
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || !Number.isSafeInteger(staleAfterMs) || staleAfterMs < 0) {
    throw new Error('Invalid raw chunk reconciliation bounds');
  }
  const intents = await transaction(client => client.query<{ id: string }>(
    `SELECT id FROM raw_chunk_blob_write_intents
     WHERE updated_at <= now() - ($1::bigint * interval '1 millisecond')
     ORDER BY updated_at, id LIMIT $2`, [staleAfterMs, Math.min(batchSize, 1000)]
  ));
  const result: RawChunkBlobReconcileResult = { selected: 0, committed: 0, deleted: 0, failed: 0, quarantined: 0 };
  // Claims recheck due time under SKIP LOCKED; selection itself owns no remote IO
  // locks. A failed or quarantined attempt moves behind other eligible records.
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(4, intents.rows.length) }, async () => {
    while (next < intents.rows.length) {
      const intent = intents.rows[next++];
      try {
        const outcome = await cleanupRawChunkWrite(storage, { id: intent.id, staleAfterMs }, transaction);
        if (outcome === 'busy') continue;
        result.selected++;
        if (outcome === 'quarantined') result.quarantined = (result.quarantined ?? 0) + 1;
        else result[outcome]++;
      } catch {
        // Wait for every worker before releasing the local run guard, even when
        // one SQL claim/settlement fails. A failed proof never authorizes deletion.
        result.selected++;
        result.failed++;
      }
    }
  }));
  return result;
}

export interface RawChunkBlobReconcilerOptions {
  batchSize: number;
  intervalMs: number;
  logger?: {
    error?(details: unknown, message?: string): void;
    info?(details: unknown, message?: string): void;
    warn?(details: unknown, message?: string): void;
  };
  reconcile?: (batchSize: number) => Promise<RawChunkBlobReconcileResult>;
}

export class RawChunkBlobReconciler {
  private active: Promise<RawChunkBlobReconcileResult> | undefined;
  private timer: NodeJS.Timeout | undefined;
  private readonly reconcile: (batchSize: number) => Promise<RawChunkBlobReconcileResult>;

  constructor(private readonly options: RawChunkBlobReconcilerOptions) {
    this.reconcile = options.reconcile ?? reconcileStaleRawChunkBlobWrites;
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.run().catch((error) => {
      this.options.logger?.error?.({ err: error }, 'Raw chunk blob reconciliation failed');
    }), this.options.intervalMs);
    void this.run().catch((error) => {
      this.options.logger?.error?.({ err: error }, 'Raw chunk blob reconciliation failed');
    });
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    if (this.active) await this.active.catch((error) => {
      this.options.logger?.warn?.({ err: error }, 'Active raw chunk blob reconciliation failed during shutdown');
    });
  }

  async run(): Promise<RawChunkBlobReconcileResult & { skipped: boolean }> {
    if (this.active) return { selected: 0, committed: 0, deleted: 0, failed: 0, skipped: true };
    const active = this.reconcile(this.options.batchSize);
    this.active = active;
    try {
      const result = await active;
      if (result.selected > 0) this.options.logger?.info?.(result, 'Raw chunk blob reconciliation completed');
      if (result.failed > 0 || (result.quarantined ?? 0) > 0) {
        this.options.logger?.warn?.(result, 'Raw chunk writes remain quarantined or require cleanup retry');
      }
      return { ...result, skipped: false };
    } finally {
      if (this.active === active) this.active = undefined;
    }
  }
}
