import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock, withTransactionMock } = vi.hoisted(() => ({
  queryMock: vi.fn(),
  withTransactionMock: vi.fn()
}));

vi.mock('../../db/client', () => ({
  query: queryMock,
  withTransaction: withTransactionMock
}));

import {
  assertCurrentWorkerLease,
  recordWorkerAction,
  releaseWorkerLease,
  renewWorkerLease,
  startWorkerLeaseHeartbeat,
  StaleWorkerLeaseError,
  type WorkerLease
} from '../worker-lease';

const extractionLease: WorkerLease = {
  queueKind: 'extraction',
  queueId: 'b3313205-c43a-48b7-b861-3679e9849928',
  claimToken: '2ce782ca-63dd-4e90-9dfa-9f7f06ea228e',
  workerId: 'worker-a'
};

describe('worker lease fencing', () => {
  afterEach(() => { vi.useRealTimers(); });
  beforeEach(() => {
    queryMock.mockReset();
    withTransactionMock.mockReset();
    withTransactionMock.mockImplementation(async (callback) => callback({ query: queryMock }));
    queryMock.mockImplementation(async (sql: string) => ({ rowCount: 1,
      rows: sql.includes(' AS live') ? [{ live: true }] : [{ deadline: '2099-01-01 00:00:00+00' }] }));
  });

  it('locks and accepts only an unexpired matching extraction claim', async () => {
    const client = { query: queryMock };
    await expect(assertCurrentWorkerLease(client as never, extractionLease)).resolves.toHaveProperty('assertUnexpired');
    expect(client.query).toHaveBeenCalledWith(expect.stringContaining('lease_expires_at::text'), [
      extractionLease.queueId, extractionLease.claimToken, extractionLease.workerId
    ]);
    expect(String(client.query.mock.calls[0][0])).toContain('FOR UPDATE');
  });

  it('rejects stale completion after takeover', async () => {
    const client = { query: vi.fn().mockResolvedValue({ rowCount: 0, rows: [] }) };
    await expect(assertCurrentWorkerLease(client as never, extractionLease)).rejects.toBeInstanceOf(StaleWorkerLeaseError);
  });

  it('renews the row and vault claims as one curation transaction', async () => {
    const lease: WorkerLease = {
      ...extractionLease,
      queueKind: 'curation',
      vaultId: 'db2a864d-a466-4384-842a-30116f0b69f1',
      vaultClaimToken: 'b36fd262-a2e5-4377-83db-39e0e3e4c078'
    };
    await expect(renewWorkerLease(lease, 60_000)).resolves.toBe(true);
    expect(queryMock).toHaveBeenCalledTimes(6);
    expect(String(queryMock.mock.calls[0][0])).toContain('FROM curation_queue');
    expect(String(queryMock.mock.calls[1][0])).toContain('curator_claim_token = $2');
    expect(String(queryMock.mock.calls[3][0])).toContain('UPDATE curation_queue');
    expect(String(queryMock.mock.calls[4][0])).toContain('UPDATE vault_curation_state');
    expect(String(queryMock.mock.calls[5][0])).toContain('clock_timestamp()');
  });

  it('does not renew a vault claim after the row claim is lost', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 0, rows: [] });
    await expect(renewWorkerLease({
      ...extractionLease,
      queueKind: 'curation',
      vaultId: 'db2a864d-a466-4384-842a-30116f0b69f1',
      vaultClaimToken: 'b36fd262-a2e5-4377-83db-39e0e3e4c078'
    }, 60_000)).resolves.toBe(false);
    expect(queryMock).toHaveBeenCalledTimes(1);
  });

  it('releases and retries only the matching capability', async () => {
    await expect(releaseWorkerLease(extractionLease, {
      incrementRetry: true,
      lastError: 'retry',
      availableAt: new Date('2026-06-01T00:00:00.000Z')
    })).resolves.toBe(true);
    expect(String(queryMock.mock.calls[0][0])).toContain('AND claim_token = $2');
    expect(queryMock.mock.calls.find(([sql]) => sql.includes('UPDATE extraction_queue'))?.[1]).toEqual([
      extractionLease.queueId,
      extractionLease.claimToken,
      extractionLease.workerId,
      1,
      'retry',
      '2026-06-01T00:00:00.000Z'
    ]);
  });

  it('records each durable action once across retries and takeovers', async () => {
    const client = { query: vi.fn().mockResolvedValueOnce({ rowCount: 1 }).mockResolvedValueOnce({ rowCount: 0 }) };
    await expect(recordWorkerAction(client as never, extractionLease, 'apply')).resolves.toBe(true);
    await expect(recordWorkerAction(client as never, { ...extractionLease, claimToken: crypto.randomUUID() }, 'apply')).resolves.toBe(false);
    expect(String(client.query.mock.calls[0][0])).toContain('ON CONFLICT (queue_kind, queue_id, action_key) DO NOTHING');
  });

  it('settles an in-flight heartbeat before stop returns, then leaves no timer', async () => {
    vi.useFakeTimers();
    let finish!: (value: boolean) => void;
    withTransactionMock.mockImplementationOnce(() => new Promise<boolean>(resolve => { finish = resolve; }));
    const heartbeat = startWorkerLeaseHeartbeat(extractionLease, 60_000, 1000);
    const renewing = heartbeat.renewNow();
    let stopped = false;
    const stop = heartbeat.stop().then(() => { stopped = true; });
    await Promise.resolve(); expect(stopped).toBe(false);
    finish(true); await renewing; await stop;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(withTransactionMock).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
    expect(await heartbeat.renewNow()).toBe(false);
  });
  it('distinguishes transient renewal failure from known loss and stops retrying a lost claim', async () => {
    vi.useFakeTimers();
    withTransactionMock.mockRejectedValueOnce(new Error('temporary DB outage')).mockResolvedValueOnce(false);
    const heartbeat = startWorkerLeaseHeartbeat(extractionLease, 60_000, 1000);
    try {
      expect(await heartbeat.renewNow()).toBe(false); expect(heartbeat.lost).toBe(false);
      expect(await heartbeat.renewNow()).toBe(false); expect(heartbeat.lost).toBe(true);
      await vi.advanceTimersByTimeAsync(5000);
      expect(withTransactionMock).toHaveBeenCalledTimes(2);
    } finally { await heartbeat.stop(); }
    expect(vi.getTimerCount()).toBe(0);
  });
});
