import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  query: vi.fn(), clientQuery: vi.fn(), transaction: vi.fn(), curate: vi.fn(), embed: vi.fn(),
  claim: vi.fn(), release: vi.fn(), shutdown: undefined as undefined | ((message: { type: string }) => void),
  finished: undefined as undefined | (() => void), auditFailure: vi.fn()
}));

vi.mock('node:worker_threads', () => ({
  parentPort: {
    on: (_event: string, listener: typeof mocks.shutdown) => { mocks.shutdown = listener; },
    postMessage: () => mocks.finished?.(), close: vi.fn()
  }
}));
vi.mock('../config', () => ({ getConfig: () => ({
  ENCRYPTION_ENABLED: false, CURATION_BATCH_SIZE: 1, CURATION_INTERVAL_MS: 60_000, STORAGE_EMBEDDING_DIMENSIONS: 2
}) }));
vi.mock('../db/client', () => ({ query: mocks.query, withTransaction: mocks.transaction, closePool: vi.fn() }));
vi.mock('../services/crypto', () => ({
  decryptForVault: async (_vault: unknown, value: string) => value,
  encryptForVault: async (_vault: unknown, value: string) => value,
  encryptSubjectForVault: vi.fn(), initCryptoClient: vi.fn(), isVaultEncryptionActive: () => false,
  prepareVaultCrypto: async () => ({ assertCurrent: async () => {}, encrypt: (_vault: unknown, value: string) => value })
}));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed: mocks.embed }) }));
vi.mock('../services/curator', () => ({
  CuratorService: class {
    prepare = (candidates: unknown[], activeMemories: unknown[]) => ({ candidates, activeMemories, deferredCandidateIds: [] });
    curatePrepared = mocks.curate;
  },
  CuratorPlanValidationError: class extends Error {},
  CuratorPreparationDeferredError: class extends Error {}
}));
vi.mock('../services/ai-resilience', () => ({ CircuitBreakerOpenError: class extends Error {} }));
vi.mock('../services/usage', () => ({ AiBudgetDeferredError: class extends Error {}, recordMemoryCountDelta: vi.fn() }));
vi.mock('../telemetry', () => ({ getSpanAttributes: (value: unknown) => value }));
vi.mock('../metrics', () => ({
  aiBudgetThrottledJobsCounter: { add: vi.fn() }, aiBudgetWaitHistogram: { record: vi.fn() },
  memoryPolicyEventCounter: { add: vi.fn() }
}));
vi.mock('../services/customer-metrics', () => ({ initCustomerMetrics: vi.fn(), shutdownCustomerMetrics: vi.fn() }));
vi.mock('../services/curation-capacity', () => ({
  claimEligibleCurationJobs: mocks.claim,
  getCuratorPlanBlockReason: () => null,
  getCuratorLimits: async () => ({ curator_candidates_per_run: 10, curator_candidates_per_call: 10,
    curator_active_memories_per_call: 10, curator_input_tokens_per_call: 0, curator_output_tokens_per_call: 0 }),
  recordCuratorDeferral: vi.fn(), recordCuratorRunCompletedActivity: vi.fn(), recordCuratorUsageInTransaction: vi.fn(),
  releaseCuratorClaim: async () => { mocks.shutdown?.({ type: 'shutdown' }); }
}));
vi.mock('../services/worker-lease', () => ({
  withWorkerLeaseTransaction: (_lease: unknown, fn: unknown) => mocks.transaction(fn),
  releaseWorkerLeaseInTransaction: vi.fn(), recordWorkerAction: async () => true, releaseWorkerLease: mocks.release,
  startWorkerLeaseHeartbeat: () => ({ stop: vi.fn() }), StaleWorkerLeaseError: class extends Error {}
}));

describe('curation worker disjoint validity application', () => {
  beforeEach(() => {
    vi.resetModules();
    for (const mock of [mocks.query, mocks.clientQuery, mocks.transaction, mocks.curate, mocks.embed,
      mocks.claim, mocks.release, mocks.auditFailure]) mock.mockReset();
    mocks.shutdown = undefined;
    mocks.finished = undefined;
    mocks.embed.mockImplementation(async () => {
      expect(mocks.transaction).not.toHaveBeenCalled();
      return [1, 0];
    });
  });

  it.each(['create', 'update'] as const)('rolls back %s before inserting, mutating or consuming either disjoint source', async mode => {
    const fixture = (id: string, status: string, validFrom: string | null, validUntil: string | null) => ({
      id, vault_id: 'vault-1', data: `fact-${id}`, subject: 'subject', subject_encrypted: null,
      subject_hmac: null, confidence: 1, salience: 0.8, sensitivity: 'low', type: 'system_fact',
      scope: 'project', scope_key: 'project-1', polarity: 'neutral', volatility: 'low',
      evidence: {}, parent_id: null, source_chunks: [], archived_at: null, status,
      valid_from: validFrom, valid_until: validUntil, row_version: `version-${id}`, total_candidates: '2'
    });
    const first = fixture('memory-1', mode === 'create' ? 'candidate' : 'active', null, '2026-05-31');
    const second = fixture('memory-2', 'candidate', '2026-06-01', null);
    const candidates = mode === 'create' ? [first, second] : [second];
    const active = mode === 'create' ? [] : [first];
    const originalSources = structuredClone([first, second]);
    const aliasToId = new Map(mode === 'create'
      ? [['C1', first.id], ['C2', second.id]] : [['M1', first.id], ['C1', second.id]]);
    const plan = {
      schema_version: 'curation-plan.v1',
      nodes_to_create: mode === 'create' ? [{ subject: 'combined', statement: 'combined fact', type: 'system_fact',
        scope: 'project', evidence: 'combined sources', consumed_candidate_ids: ['C1', 'C2'] }] : [],
      nodes_to_update: mode === 'update' ? [{ id: 'M1', statement: 'combined fact',
        reason: 'combined sources', consumed_candidate_ids: ['C1'] }] : [],
      edges_to_create: [], nodes_to_archive: [], promoted_candidates: [], discarded_candidates: []
    };
    mocks.claim.mockResolvedValue([{ queue_id: 'queue-1', vault_id: 'vault-1', segment_id: 'segment-1',
      claim_token: 'claim-1', vault_claim_token: 'vault-claim-1' }]);
    mocks.curate.mockResolvedValue({ result: plan, rawResponse: plan,
      graph: { creationOrder: mode === 'create' ? [0] : [], parents: mode === 'create' ? [null] : [], edges: [] },
      aliasMaps: { aliasToId, idToAlias: new Map([...aliasToId].map(([alias, id]) => [id, alias])) },
      audit: { model: 'test', schemaVersion: 'curation-plan.v1', promptVersion: 'test', promptHash: 'a'.repeat(64) }
    });
    mocks.query.mockImplementation(async (sql: string, params: unknown[]) => {
      if (sql.includes('SELECT retry_count')) return { rowCount: 1, rows: [{ retry_count: 0 }] };
      if (sql.includes('FROM vaults')) return { rowCount: 1, rows: [{ id: 'vault-1', type: 'general', account_id: null }] };
      if (sql.includes('FROM segments')) return { rowCount: 1, rows: [{ context: null, session_id: 'session-1', project_id: 'project-1', task_id: null }] };
      if (sql.includes('COUNT(*) OVER()')) return { rowCount: candidates.length, rows: candidates };
      if (sql.includes('WITH candidate_context')) return { rowCount: active.length, rows: active };
      if (sql.includes('INSERT INTO curation_review_runs')) return { rowCount: 1, rows: [{ id: 'review-1' }] };
      if (sql.includes("validation_status = 'application_failed'")) {
        mocks.auditFailure(params);
        return { rowCount: 1, rows: [] };
      }
      throw new Error(`Unexpected worker query: ${sql}`);
    });
    mocks.clientQuery.mockImplementation(async (sql: string) => {
      if (sql.includes('FOR UPDATE')) return { rowCount: 2, rows: [first, second] };
      if (sql.includes('UPDATE curation_review_runs') || sql.includes('INSERT INTO curation_action_log')) {
        return { rowCount: 1, rows: [] };
      }
      if (['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)) return { rowCount: null, rows: [] };
      throw new Error(`Unexpected application query: ${sql}`);
    });
    mocks.transaction.mockImplementation(async run => {
      await mocks.clientQuery('BEGIN');
      try {
        const result = await run({ query: mocks.clientQuery });
        await mocks.clientQuery('COMMIT');
        return result;
      } catch (error) {
        await mocks.clientQuery('ROLLBACK');
        throw error;
      }
    });
    const finished = new Promise<void>(resolve => { mocks.finished = resolve; });
    const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      await import('./curation-worker');
      await finished;

      expect(mocks.curate).toHaveBeenCalledOnce();
      expect(mocks.transaction).toHaveBeenCalledOnce();
      expect(mocks.clientQuery.mock.calls.map(([sql]) => sql)).toContain('ROLLBACK');
      expect(mocks.clientQuery.mock.calls.map(([sql]) => sql)).not.toContain('COMMIT');
      const writes = [...mocks.clientQuery.mock.calls, ...mocks.query.mock.calls].map(([sql]) => String(sql));
      expect(writes.some(sql => /(?:INSERT INTO|UPDATE|DELETE FROM) memories\b/.test(sql))).toBe(false);
      expect(writes.some(sql => /DELETE FROM curation_queue\b/.test(sql))).toBe(false);
      expect(mocks.embed).toHaveBeenCalledOnce(); // Preparation is outside the rejected transaction.
      expect([first, second]).toEqual(originalSources);
      expect(mocks.auditFailure).toHaveBeenCalledWith(['review-1',
        JSON.stringify(['Validity windows do not overlap: 2026-06-01 > 2026-05-31'])]);
      expect(mocks.release).toHaveBeenCalledWith(expect.any(Object), {
        incrementRetry: true, lastError: 'Validity windows do not overlap: 2026-06-01 > 2026-05-31'
      });
    } finally {
      mocks.shutdown?.({ type: 'shutdown' });
      errorLog.mockRestore();
    }
  });
});
