import type { PoolClient, QueryResultRow } from 'pg';

import type { EventPublisher } from '../events/event-publisher';
import {
  buildPlatformEvent,
  legacyUsagePeriodClosedEventType,
  schemaUrlForEventType,
  usagePeriodClosedEventType,
  type JsonObject,
  type PlatformEvent
} from '../events/platform-event';
import { pool } from '../db/client';
import {
  eventOutboxDispatchCounter,
  eventOutboxLagHistogram,
  eventOutboxPublishFailureCounter
} from '../metrics';

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

interface OutboxDepthRow extends QueryResultRow {
  depth: string;
  oldest_pending_age_ms: string | null;
}

interface DispatcherDb {
  connect(): Promise<Pick<PoolClient, 'query' | 'release'>>;
}

export interface EventOutboxDispatcherLogger {
  error?(details: unknown, message?: string): void;
  info?(details: unknown, message?: string): void;
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
  publisherName?: string;
  retryBaseDelayMs: number;
  warnDepthThreshold?: number;
  warnOldestAgeMs?: number;
}

export interface EventOutboxDispatchResult {
  dead: number;
  delivered: number;
  failed: number;
  oldestPendingAgeMs: number | null;
  outboxDepth: number;
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
  private readonly publisherName: string;
  private readonly retryBaseDelayMs: number;
  private readonly warnDepthThreshold: number;
  private readonly warnOldestAgeMs: number;
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
    this.publisherName = options.publisherName ?? 'unknown';
    this.retryBaseDelayMs = options.retryBaseDelayMs;
    this.warnDepthThreshold = options.warnDepthThreshold ?? 100;
    this.warnOldestAgeMs = options.warnOldestAgeMs ?? 15 * 60 * 1000;
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

      const depth = await this.loadOutboxDepth(client);
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
        oldestPendingAgeMs: depth.oldestPendingAgeMs,
        outboxDepth: depth.outboxDepth,
        selected: result.rows.length,
        skipped: false
      };

      for (const row of result.rows) {
        await this.dispatchRow(client, row, stats);
      }

      this.recordDispatchCycle(stats);
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

  private async loadOutboxDepth(client: Pick<PoolClient, 'query'>): Promise<Pick<EventOutboxDispatchResult, 'oldestPendingAgeMs' | 'outboxDepth'>> {
    const result = await client.query<OutboxDepthRow>(
      `SELECT COUNT(*)::text AS depth,
              EXTRACT(EPOCH FROM (now() - MIN(created_at))) * 1000 AS oldest_pending_age_ms
       FROM platform_event_outbox
       WHERE status IN ('pending', 'failed')`
    );

    return {
      outboxDepth: Number(result.rows[0]?.depth ?? 0),
      oldestPendingAgeMs: result.rows[0]?.oldest_pending_age_ms === null || result.rows[0]?.oldest_pending_age_ms === undefined
        ? null
        : Number(result.rows[0].oldest_pending_age_ms)
    };
  }

  private recordDispatchCycle(stats: EventOutboxDispatchResult): void {
    const attributes = { publisher: this.publisherName };
    eventOutboxDispatchCounter.add(1, { ...attributes, outcome: 'cycle' });
    eventOutboxDispatchCounter.add(stats.selected, { ...attributes, outcome: 'attempted' });
    eventOutboxDispatchCounter.add(stats.delivered, { ...attributes, outcome: 'delivered' });
    eventOutboxDispatchCounter.add(stats.failed, { ...attributes, outcome: 'failed' });
    eventOutboxDispatchCounter.add(stats.dead, { ...attributes, outcome: 'dead' });
    if (stats.oldestPendingAgeMs !== null) {
      eventOutboxLagHistogram.record(stats.oldestPendingAgeMs, attributes);
    }

    const logDetails = {
      dead: stats.dead,
      delivered: stats.delivered,
      failed: stats.failed,
      oldest_pending_age_ms: stats.oldestPendingAgeMs,
      outbox_depth: stats.outboxDepth,
      publisher: this.publisherName,
      selected: stats.selected
    };

    if (
      stats.outboxDepth > this.warnDepthThreshold ||
      (stats.oldestPendingAgeMs !== null && stats.oldestPendingAgeMs > this.warnOldestAgeMs)
    ) {
      this.logger?.warn?.({
        ...logDetails,
        warn_depth_threshold: this.warnDepthThreshold,
        warn_oldest_age_ms: this.warnOldestAgeMs
      }, 'Event outbox backlog above warning threshold');
      return;
    }

    this.logger?.info?.(logDetails, 'Event outbox dispatch cycle completed');
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
        eventOutboxPublishFailureCounter.add(1, {
          event_type: row.event_type,
          publisher: this.publisherName,
          status: 'dead'
        });
        this.logger?.error?.({
          adapter: this.publisherName,
          err: error,
          event_id: row.event_id,
          event_type: row.event_type,
          attempts
        }, 'Event outbox row dead-lettered');
      } else {
        stats.failed += 1;
        eventOutboxPublishFailureCounter.add(1, {
          event_type: row.event_type,
          publisher: this.publisherName,
          status: 'failed'
        });
        this.logger?.warn?.({
          adapter: this.publisherName,
          err: error,
          event_id: row.event_id,
          event_type: row.event_type,
          attempts,
          retryDelayMs
        }, 'Event outbox publish failed');
      }
    }
  }
}

export function calculateRetryDelayMs(attempts: number, baseDelayMs: number, maxDelayMs: number): number {
  return Math.min(maxDelayMs, Math.max(1, baseDelayMs) * 2 ** Math.max(0, attempts));
}

function rowToPlatformEvent(row: EventOutboxRow): PlatformEvent<string> {
  const eventType = row.event_type === legacyUsagePeriodClosedEventType
    ? usagePeriodClosedEventType
    : row.event_type;
  const occurredAt = row.occurred_at instanceof Date ? row.occurred_at : new Date(row.occurred_at);
  const workspaceId = readString(row.payload.workspace_id) ?? readString(row.payload.account_id) ?? 'unknown';
  const vaultId = readString(row.payload.vault_id) ?? readString(row.payload.platform_vault_id);
  const category = eventType === usagePeriodClosedEventType ? 'usage' : readCategory(row.payload.category);

  return {
    ...buildPlatformEvent({
      category,
      data: row.payload,
      id: row.event_id,
      occurredAt,
      severity: readSeverity(row.payload.severity),
      subject: normalizeSubject(row.subject),
      type: eventType,
      vaultId,
      workspaceId
    }),
    dataschema: schemaUrlForEventType(eventType)
  };
}

function normalizeSubject(subject: string): string {
  return subject
    .replace(/^vault:/, 'vault/')
    .replace('/memory:', '/memory/');
}

function readString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function readCategory(value: unknown): PlatformEvent<string>['category'] {
  return value === 'activity' || value === 'operational' || value === 'security' || value === 'usage'
    ? value
    : 'activity';
}

function readSeverity(value: unknown): PlatformEvent<string>['severity'] {
  return value === 'error' || value === 'info' || value === 'notice' || value === 'warning'
    ? value
    : 'info';
}

function emptyDispatchResult(skipped: boolean): EventOutboxDispatchResult {
  return {
    dead: 0,
    delivered: 0,
    failed: 0,
    oldestPendingAgeMs: null,
    outboxDepth: 0,
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
