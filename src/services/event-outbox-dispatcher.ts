import type { PoolClient, QueryResultRow } from 'pg';

import type { EventPublisher } from '../events/event-publisher';
import type { JsonObject, PlatformEvent } from '../events/platform-event';
import { pool } from '../db/client';

const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_LOCK_KEY = 4827_166;

interface EventOutboxRow extends QueryResultRow {
  attempts: number;
  event_id: string;
  event_type: string;
  id: string;
  occurred_at: Date | string;
  payload: JsonObject;
  schema_version: number;
  subject: string;
}

interface AdvisoryLockRow extends QueryResultRow {
  locked: boolean;
}

interface DispatcherDb {
  connect(): Promise<Pick<PoolClient, 'query' | 'release'>>;
}

export interface EventOutboxDispatcherLogger {
  error?(details: unknown, message?: string): void;
  warn?(details: unknown, message?: string): void;
}

export interface EventOutboxDispatcherOptions {
  batchSize?: number;
  db?: DispatcherDb;
  intervalMs: number;
  lockKey?: number;
  logger?: EventOutboxDispatcherLogger;
  maxAttempts: number;
  maxRetryDelayMs: number;
  publisher: EventPublisher;
  retryBaseDelayMs: number;
}

export interface EventOutboxDispatchResult {
  dead: number;
  delivered: number;
  failed: number;
  selected: number;
  skipped: boolean;
}

export class EventOutboxDispatcher {
  private readonly batchSize: number;
  private readonly db: DispatcherDb;
  private readonly intervalMs: number;
  private readonly lockKey: number;
  private readonly logger?: EventOutboxDispatcherLogger;
  private readonly maxAttempts: number;
  private readonly maxRetryDelayMs: number;
  private readonly publisher: EventPublisher;
  private readonly retryBaseDelayMs: number;
  private activeDispatch: Promise<EventOutboxDispatchResult> | undefined;
  private timer: NodeJS.Timeout | undefined;

  constructor(options: EventOutboxDispatcherOptions) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.db = options.db ?? pool;
    this.intervalMs = options.intervalMs;
    this.lockKey = options.lockKey ?? DEFAULT_LOCK_KEY;
    this.logger = options.logger;
    this.maxAttempts = options.maxAttempts;
    this.maxRetryDelayMs = options.maxRetryDelayMs;
    this.publisher = options.publisher;
    this.retryBaseDelayMs = options.retryBaseDelayMs;
  }

  start(): void {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(() => {
      void this.dispatchDueEvents().catch((error: unknown) => {
        this.logger?.error?.({ err: error }, 'Event outbox dispatch failed');
      });
    }, this.intervalMs);

    void this.dispatchDueEvents().catch((error: unknown) => {
      this.logger?.error?.({ err: error }, 'Event outbox dispatch failed');
    });
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }

    if (this.activeDispatch) {
      try {
        await this.activeDispatch;
      } catch (error) {
        this.logger?.warn?.({ err: error }, 'Active event outbox dispatch failed during shutdown');
      }
    }
  }

  async dispatchDueEvents(): Promise<EventOutboxDispatchResult> {
    if (this.activeDispatch) {
      return emptyDispatchResult(true);
    }

    const dispatch = this.dispatchDueEventsOnce();
    this.activeDispatch = dispatch;

    try {
      return await dispatch;
    } finally {
      if (this.activeDispatch === dispatch) {
        this.activeDispatch = undefined;
      }
    }
  }

  private async dispatchDueEventsOnce(): Promise<EventOutboxDispatchResult> {
    let client: Pick<PoolClient, 'query' | 'release'> | undefined;
    let lockHeld = false;

    try {
      client = await this.db.connect();
      const lockResult = await client.query<AdvisoryLockRow>(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [this.lockKey]
      );
      lockHeld = lockResult.rows[0]?.locked === true;
      if (!lockHeld) {
        return emptyDispatchResult(true);
      }

      const result = await client.query<EventOutboxRow>(
        `SELECT id, event_id::text, event_type, schema_version, occurred_at, subject, payload, attempts
         FROM platform_event_outbox
         WHERE status = 'pending'
            OR (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= now()))
         ORDER BY created_at ASC
         LIMIT $1`,
        [this.batchSize]
      );
      const stats: EventOutboxDispatchResult = {
        dead: 0,
        delivered: 0,
        failed: 0,
        selected: result.rows.length,
        skipped: false
      };

      for (const row of result.rows) {
        await this.dispatchRow(client, row, stats);
      }

      return stats;
    } finally {
      if (lockHeld) {
        try {
          await client?.query('SELECT pg_advisory_unlock($1)', [this.lockKey]);
        } catch (error) {
          this.logger?.warn?.({ err: error }, 'Failed to release event outbox dispatcher lock');
        }
      }
      client?.release();
    }
  }

  private async dispatchRow(
    client: Pick<PoolClient, 'query'>,
    row: EventOutboxRow,
    stats: EventOutboxDispatchResult
  ): Promise<void> {
    try {
      await this.publisher.publish(rowToPlatformEvent(row));
      await client.query(
        `UPDATE platform_event_outbox
         SET status = 'delivered',
             last_attempted_at = now(),
             last_error = NULL,
             next_retry_at = NULL
         WHERE id = $1`,
        [row.id]
      );
      stats.delivered += 1;
    } catch (error) {
      const attempts = Number(row.attempts) + 1;
      const isDead = attempts >= this.maxAttempts;
      const status = isDead ? 'dead' : 'failed';
      const retryDelayMs = isDead ? null : calculateRetryDelayMs(
        attempts,
        this.retryBaseDelayMs,
        this.maxRetryDelayMs
      );

      await client.query(
        `UPDATE platform_event_outbox
         SET status = $2,
             attempts = $3,
             last_attempted_at = now(),
             last_error = $4,
             next_retry_at = CASE
               WHEN $5::int IS NULL THEN NULL
               ELSE now() + ($5::int * interval '1 millisecond')
             END
         WHERE id = $1`,
        [row.id, status, attempts, errorToMessage(error), retryDelayMs]
      );

      if (isDead) {
        stats.dead += 1;
        this.logger?.error?.({ err: error, event_id: row.event_id, attempts }, 'Event outbox row dead-lettered');
      } else {
        stats.failed += 1;
        this.logger?.warn?.({ err: error, event_id: row.event_id, attempts, retryDelayMs }, 'Event outbox publish failed');
      }
    }
  }
}

export function calculateRetryDelayMs(attempts: number, baseDelayMs: number, maxDelayMs: number): number {
  return Math.min(maxDelayMs, Math.max(1, baseDelayMs) * 2 ** Math.max(0, attempts));
}

function rowToPlatformEvent(row: EventOutboxRow): PlatformEvent<string> {
  return {
    event_id: row.event_id,
    event_type: row.event_type,
    schema_version: Number(row.schema_version),
    occurred_at: row.occurred_at instanceof Date ? row.occurred_at.toISOString() : new Date(row.occurred_at).toISOString(),
    subject: row.subject,
    payload: row.payload
  };
}

function emptyDispatchResult(skipped: boolean): EventOutboxDispatchResult {
  return {
    dead: 0,
    delivered: 0,
    failed: 0,
    selected: 0,
    skipped
  };
}

function errorToMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}
