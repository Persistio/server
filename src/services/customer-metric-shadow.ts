import type { Pool, PoolClient } from 'pg';
import { randomUUID } from 'node:crypto';
import { getConfig } from '../config';
import { type CustomerMetricEvent, type CustomerMetricPublisher } from './customer-metrics';
import { workerWakeQuery } from './worker-wake-query';

/** Requires the business transaction's checked-out client, never a Pool. No network
 * publication here. An insert failure aborts uncommitted business work. The caller
 * prepares the complete envelope once and retains it for legacy emission too. */
export async function captureShadowCustomerMetric(
  client: PoolClient, event: CustomerMetricEvent,
  enabled = getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE
): Promise<void> {
  if (!enabled) return;
  if (!event.workspace_id) return;
  const inserted = await client.query(
    `INSERT INTO customer_metric_shadow_outbox(event_id,event_ts,payload) VALUES($1,$2,$3::jsonb)
     ON CONFLICT(event_id) DO NOTHING RETURNING event_id`,
    [event.event_id, event.event_ts, JSON.stringify(event)]);
  if (!inserted.rowCount) {
    const same = await client.query(`SELECT 1 FROM customer_metric_shadow_outbox
      WHERE event_id=$1 AND payload=$2::jsonb`, [event.event_id, JSON.stringify(event)]);
    if (same.rowCount !== 1) throw new Error('Shadow event identity conflict');
  }
}

interface Claimed { event_id: string; payload: CustomerMetricEvent; attempts: number }
export interface ShadowDeliveryReport { delivered: number; retried: number; stale: number; dead: number; purged: number }

/** One pass only. Caller owns publisher lifetime and its enclosing request deadline.
 * On timeout the publisher is poisoned for this dispatcher instance: an SDK send may
 * still finish, so no more calls are admitted. Retry uses the same durable event ID. */
export class ShadowCustomerMetricDispatcher {
  private busy = false;
  private poisoned = false;
  constructor(
    private readonly db: Pick<Pool, 'connect'>,
    private readonly publisher: CustomerMetricPublisher,
    private readonly options: { batchSize?: number; publishTimeoutMs?: number } = {}
  ) {
    const { batchSize = 20, publishTimeoutMs = 5000 } = options;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 100 ||
        !Number.isInteger(publishTimeoutMs) || publishTimeoutMs < 1 || publishTimeoutMs > 10000) {
      throw new Error('Invalid shadow delivery bounds');
    }
  }

  async dispatchOnce(): Promise<ShadowDeliveryReport> {
    if (this.busy || this.poisoned) throw new Error('Shadow publisher unavailable');
    this.busy = true;
    const report = { delivered: 0, retried: 0, stale: 0, dead: 0, purged: 0 };
    const batch = this.options.batchSize ?? 20;
    const deadline = Date.now() + 20_000;
    try {
      // No silent delivery outside the hourly lookback. Dead rows need reviewed backfill.
      report.dead = (await workerWakeQuery(this.db, `WITH expired AS (
        SELECT event_id FROM customer_metric_shadow_outbox WHERE status='pending'
          AND (lease_expires_at IS NULL OR lease_expires_at<=now())
          AND (attempts>=10 OR event_ts<=now()-interval '6 hours')
        ORDER BY available_at,event_id LIMIT $1 FOR UPDATE SKIP LOCKED)
        UPDATE customer_metric_shadow_outbox o SET status='dead',settled_at=now(),lease_owner=NULL,lease_expires_at=NULL
        FROM expired e WHERE o.event_id=e.event_id`, [batch])).rowCount ?? 0;
      report.purged = (await workerWakeQuery(this.db, `WITH expired AS (
        SELECT event_id FROM customer_metric_shadow_outbox WHERE settled_at<now()-interval '30 days'
        ORDER BY settled_at LIMIT $1 FOR UPDATE SKIP LOCKED)
        DELETE FROM customer_metric_shadow_outbox o USING expired e WHERE o.event_id=e.event_id`, [batch])).rowCount ?? 0;
      // Claim one row at a time so no batch waits behind another row's network call.
      for (let i = 0; i < batch && Date.now() + 10_000 < deadline; i++) {
        const owner = randomUUID();
        const rows = (await workerWakeQuery<Claimed>(this.db, `WITH due AS (
          SELECT event_id FROM customer_metric_shadow_outbox WHERE status='pending' AND attempts<10
            AND event_ts>now()-interval '6 hours' AND available_at<=now()
            AND (lease_expires_at IS NULL OR lease_expires_at<=now())
          ORDER BY available_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED)
          UPDATE customer_metric_shadow_outbox o SET lease_owner=$1,lease_expires_at=clock_timestamp()+interval '30 seconds',attempts=attempts+1
          FROM due WHERE o.event_id=due.event_id RETURNING o.event_id,o.payload,o.attempts`, [owner])).rows;
        const row = rows[0];
        if (!row) break;
        let timer: NodeJS.Timeout | undefined;
        let published = false;
        try {
          await Promise.race([
            this.publisher.publish([row.payload]),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => { this.poisoned = true; reject(new Error('Shadow publish deadline')); },
                this.options.publishTimeoutMs ?? 5000);
            })
          ]);
          published = true;
        } catch { /* Metadata-only status; retain original payload for ambiguous publish. */ }
        finally { if (timer) clearTimeout(timer); }
        const settled = await workerWakeQuery(this.db, `UPDATE customer_metric_shadow_outbox
          SET status=$3,settled_at=CASE WHEN $3='pending' THEN NULL ELSE now() END,
            lease_owner=NULL,lease_expires_at=NULL,available_at=now()+($4::int*interval '1 second')
          WHERE event_id=$1 AND lease_owner=$2 AND lease_expires_at>clock_timestamp() AND status='pending'`,
          [row.event_id, owner, published ? 'delivered' : row.attempts >= 10 ? 'dead' : 'pending',
            Math.min(300, 2 ** row.attempts)]);
        if (!settled.rowCount) report.stale++;
        else if (published) report.delivered++;
        else if (row.attempts >= 10) report.dead++;
        else report.retried++;
        if (this.poisoned) break;
      }
      return report;
    } finally { this.busy = false; }
  }
}
