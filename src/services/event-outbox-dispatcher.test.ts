import { describe, expect, it, vi } from 'vitest';

import type { EventPublisher } from '../events/event-publisher';
import { EventOutboxDispatcher, calculateRetryDelayMs } from './event-outbox-dispatcher';

function createClient(rows: unknown[]) {
  const query = vi.fn()
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ locked: true }] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ depth: String(rows.length), oldest_pending_age_ms: rows.length ? '3000' : null }] })
    .mockResolvedValueOnce({ rowCount: rows.length, rows })
    .mockResolvedValue({ rowCount: 1, rows: [] });

  return {
    client: {
      query,
      release: vi.fn()
    },
    connect: vi.fn()
  };
}

function outboxRow(overrides: Record<string, unknown> = {}) {
  return {
    attempts: 0,
    event_id: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
    event_type: 'vault.usage_period.closed',
    id: 'row-1',
    occurred_at: new Date('2026-06-01T00:00:03.000Z'),
    payload: {
      account_id: 'workspace-1',
      period: '2026-05',
      platform_vault_id: 'vault-1',
      plan_id: 'unlimited',
      usage: { ingest_events: 1 },
      limits: { ingest_events_per_month: 10 }
    },
    schema_version: 1,
    subject: 'vault:vault-1',
    ...overrides
  };
}

function createDispatcher(input: {
  logger?: ConstructorParameters<typeof EventOutboxDispatcher>[0]['logger'];
  publisher: EventPublisher;
  rows: unknown[];
  warnDepthThreshold?: number;
  warnOldestAgeMs?: number;
}) {
  const { client, connect } = createClient(input.rows);
  connect.mockResolvedValue(client);
  const dispatcher = new EventOutboxDispatcher({
    db: { connect },
    intervalMs: 10000,
    maxAttempts: 5,
    maxRetryDelayMs: 300000,
    logger: input.logger,
    publisher: input.publisher,
    publisherName: 'webhook',
    retryBaseDelayMs: 1000,
    warnDepthThreshold: input.warnDepthThreshold,
    warnOldestAgeMs: input.warnOldestAgeMs
  });

  return { client, connect, dispatcher };
}

describe('EventOutboxDispatcher', () => {
  it('publishes due rows and marks successful delivery', async () => {
    const publish = vi.fn().mockResolvedValue(undefined);
    const { client, dispatcher } = createDispatcher({
      publisher: { publish },
      rows: [outboxRow()]
    });

    await expect(dispatcher.dispatchDueEvents()).resolves.toEqual({
      dead: 0,
      delivered: 1,
      failed: 0,
      oldestPendingAgeMs: 3000,
      outboxDepth: 1,
      selected: 1,
      skipped: false
    });

    expect(publish).toHaveBeenCalledWith({
      category: 'usage',
      data: expect.objectContaining({ period: '2026-05' }),
      datacontenttype: 'application/json',
      dataschema: 'https://schemas.persistio.io/events/io.persistio.usage_period.closed.v1.json',
      id: '5a3b3e77-cbd8-48f3-98fd-095f8fcb6070',
      severity: 'info',
      source: 'io.persistio.platform',
      specversion: '1.0',
      subject: 'vault/vault-1',
      time: '2026-06-01T00:00:03.000Z',
      type: 'io.persistio.usage_period.closed.v1',
      vaultid: 'vault-1',
      workspaceid: 'workspace-1'
    });
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining("SET status = 'delivered'"),
      ['row-1']
    );
    expect(client.query).toHaveBeenCalledWith('SELECT pg_advisory_unlock($1)', [4827166]);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it('records failed delivery with incremented attempts and retry backoff', async () => {
    const publish = vi.fn().mockRejectedValue(new Error('transport unavailable'));
    const { client, dispatcher } = createDispatcher({
      publisher: { publish },
      rows: [outboxRow({ attempts: 1 })]
    });

    await expect(dispatcher.dispatchDueEvents()).resolves.toMatchObject({
      dead: 0,
      delivered: 0,
      failed: 1,
      outboxDepth: 1,
      selected: 1
    });

    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('next_retry_at = CASE'),
      ['row-1', 'failed', 2, 'transport unavailable', 4000]
    );
  });

  it('marks rows dead after max attempts', async () => {
    const publish = vi.fn().mockRejectedValue(new Error('still unavailable'));
    const { client, dispatcher } = createDispatcher({
      publisher: { publish },
      rows: [outboxRow({ attempts: 4 })]
    });

    await expect(dispatcher.dispatchDueEvents()).resolves.toMatchObject({
      dead: 1,
      delivered: 0,
      failed: 0,
      outboxDepth: 1,
      selected: 1
    });

    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('next_retry_at = CASE'),
      ['row-1', 'dead', 5, 'still unavailable', null]
    );
  });

  it('does not overlap concurrent dispatch batches', async () => {
    let releasePublish: (() => void) | undefined;
    const publish = vi.fn(() => new Promise<void>((resolve) => {
      releasePublish = resolve;
    }));
    const { dispatcher } = createDispatcher({
      publisher: { publish },
      rows: [outboxRow()]
    });

    const first = dispatcher.dispatchDueEvents();
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    await expect(dispatcher.dispatchDueEvents()).resolves.toMatchObject({
      oldestPendingAgeMs: null,
      outboxDepth: 0,
      skipped: true,
      selected: 0
    });

    releasePublish?.();
    await first;
  });

  it('waits for active dispatch work before stopping', async () => {
    let releasePublish: (() => void) | undefined;
    const publish = vi.fn(() => new Promise<void>((resolve) => {
      releasePublish = resolve;
    }));
    const { dispatcher } = createDispatcher({
      publisher: { publish },
      rows: [outboxRow()]
    });

    const dispatch = dispatcher.dispatchDueEvents();
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    let stopped = false;
    const stop = dispatcher.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();

    expect(stopped).toBe(false);

    releasePublish?.();
    await dispatch;
    await stop;

    expect(stopped).toBe(true);
  });

  it('resets the local dispatch guard when connecting fails', async () => {
    const { client } = createClient([outboxRow()]);
    const connect = vi.fn()
      .mockRejectedValueOnce(new Error('pool exhausted'))
      .mockResolvedValueOnce(client);
    const publish = vi.fn().mockResolvedValue(undefined);
    const dispatcher = new EventOutboxDispatcher({
      db: { connect },
      intervalMs: 10000,
      maxAttempts: 5,
      maxRetryDelayMs: 300000,
      publisher: { publish },
      retryBaseDelayMs: 1000
    });

    await expect(dispatcher.dispatchDueEvents()).rejects.toThrow('pool exhausted');
    await expect(dispatcher.dispatchDueEvents()).resolves.toMatchObject({
      delivered: 1,
      outboxDepth: 1,
      skipped: false
    });
    expect(publish).toHaveBeenCalledOnce();
  });

  it('skips dispatch when the advisory lock is held elsewhere', async () => {
    const client = {
      query: vi.fn()
        .mockResolvedValueOnce({ rowCount: 1, rows: [{ locked: false }] })
        .mockResolvedValue({ rowCount: 1, rows: [] }),
      release: vi.fn()
    };
    const dispatcher = new EventOutboxDispatcher({
      db: { connect: vi.fn().mockResolvedValue(client) },
      intervalMs: 10000,
      maxAttempts: 5,
      maxRetryDelayMs: 300000,
      publisher: { publish: vi.fn() },
      retryBaseDelayMs: 1000
    });

    await expect(dispatcher.dispatchDueEvents()).resolves.toMatchObject({
      skipped: true,
      selected: 0
    });

    expect(client.query).not.toHaveBeenCalledWith('SELECT pg_advisory_unlock($1)', [4827166]);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it('logs event outbox depth and dispatch cycle outcomes', async () => {
    const logger = {
      info: vi.fn(),
      warn: vi.fn()
    };
    const { dispatcher } = createDispatcher({
      logger,
      publisher: { publish: vi.fn().mockResolvedValue(undefined) },
      rows: [outboxRow()]
    });

    await dispatcher.dispatchDueEvents();

    expect(logger.info).toHaveBeenCalledWith(expect.objectContaining({
      delivered: 1,
      failed: 0,
      oldest_pending_age_ms: 3000,
      outbox_depth: 1,
      publisher: 'webhook',
      selected: 1
    }), 'Event outbox dispatch cycle completed');
    expect(logger.warn).not.toHaveBeenCalledWith(expect.anything(), 'Event outbox backlog above warning threshold');
  });

  it('warns when event outbox lag exceeds the configured threshold', async () => {
    const logger = {
      info: vi.fn(),
      warn: vi.fn()
    };
    const { dispatcher } = createDispatcher({
      logger,
      publisher: { publish: vi.fn().mockResolvedValue(undefined) },
      rows: [outboxRow()],
      warnOldestAgeMs: 1000
    });

    await dispatcher.dispatchDueEvents();

    expect(logger.warn).toHaveBeenCalledWith(expect.objectContaining({
      oldest_pending_age_ms: 3000,
      outbox_depth: 1,
      warn_oldest_age_ms: 1000
    }), 'Event outbox backlog above warning threshold');
    expect(logger.info).not.toHaveBeenCalledWith(expect.anything(), 'Event outbox dispatch cycle completed');
  });

  it('caps exponential retry delay', () => {
    expect(calculateRetryDelayMs(1, 1000, 300000)).toBe(2000);
    expect(calculateRetryDelayMs(20, 1000, 300000)).toBe(300000);
  });
});
