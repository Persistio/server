import { beforeEach, describe, expect, it, vi } from 'vitest';

const { decryptForVaultMock, enforceMemoryCreationLimitMock, extractorMock, recordMemoryCountDeltaMock } = vi.hoisted(() => ({
  decryptForVaultMock: vi.fn(),
  enforceMemoryCreationLimitMock: vi.fn(),
  extractorMock: {
    arbitrateConflict: vi.fn()
  },
  recordMemoryCountDeltaMock: vi.fn()
}));

vi.mock('../crypto', () => ({
  computeSubjectHmac: vi.fn(() => 'subject-hmac'),
  decryptForVault: decryptForVaultMock,
  encryptForVault: vi.fn(async (_vault, value: string) => value),
  encryptSubjectForVault: vi.fn(async () => null),
  isVaultEncryptionActive: vi.fn(() => false)
}));

vi.mock('../extractor', () => ({ ExtractorService: class { arbitrateConflict = extractorMock.arbitrateConflict; } }));

vi.mock('../entity-resolver', () => ({
  normaliseSubject: vi.fn((subject: string) => subject.toLowerCase().trim()),
  resolveCanonical: vi.fn(async () => null)
}));

vi.mock('../usage', () => ({
  reserveMemoryCreationInTransaction: enforceMemoryCreationLimitMock,
  recordCommittedApiQuotaReservation: vi.fn(),
  recordMemoryCountDelta: recordMemoryCountDeltaMock
}));

vi.mock('../../telemetry', () => ({
  meter: {
    createCounter: vi.fn(() => ({ add: vi.fn() })),
    createHistogram: vi.fn(() => ({ record: vi.fn() })),
    createObservableGauge: vi.fn(() => ({ addCallback: vi.fn() }))
  },
  withSpan: async (_name: string, _attributes: Record<string, unknown>, fn: (span: {
    setAttribute: (key: string, value: string | number | boolean) => void;
  }) => Promise<unknown>) => fn({ setAttribute: vi.fn() })
}));

import { deduplicateMemoryInTransaction, fingerprintDedupInput, getDedupEscalationRequest, type DedupInput, type DedupOptions } from '../dedup';
import type { WorkerEffect } from '../worker-effects';
const effects: WorkerEffect[] = [];
const preparedCrypto = {
  assertCurrent: vi.fn(async () => {}),
  encrypt: (_vault: unknown, value: string) => value,
  decrypt: (_vault: unknown, value: string) => value,
  subject: () => null,
  subjectMatch: (_vault: unknown, value: string) => value
};
const apply = (value: DedupInput, db: ReturnType<typeof createDb>, options: DedupOptions = {}) =>
  deduplicateMemoryInTransaction(value, db as never, preparedCrypto, effects,
    { precomputedConflictInput: fingerprintDedupInput(value), ...options });

function input(): DedupInput {
  return {
    vaultId: 'vault-1',
    fact: 'User prefers batched escalation.',
    score: 8,
    subject: 'User',
    embedding: [0.1, 0.2],
    sourceChunks: ['00000000-0000-0000-0000-000000000001'],
    salience: 0.8,
    sensitivity: 'low',
    type: 'user_preference',
    scope: 'global',
    scopeKey: null,
    polarity: 'neutral',
    status: 'active',
    volatility: 'low',
    evidence: null,
    validFrom: null,
    validUntil: null,
    sourceSegmentId: null
  };
}

function createDb(options: {
  exactMatch?: { scope: DedupInput['scope']; status: DedupInput['status']; evidence?: unknown };
  hasSubjectMatch?: boolean;
  matchEvidence?: unknown;
  matchScope?: DedupInput['scope'];
  matchStatus?: DedupInput['status'];
  similarity?: number;
} = {}) {
  const {
    exactMatch,
    hasSubjectMatch = true,
    matchEvidence = null,
    matchScope = 'project',
    matchStatus = 'active',
    similarity = 0.82
  } = options;
  return {
    query: vi.fn(async (sql: string) => {
      if (sql.includes('FROM vaults')) {
        return {
          rowCount: 1,
          rows: [{ id: 'vault-1', account_id: 'account-1', encrypted_dek: null, vault_encryption_enabled: false }]
        };
      }

      if (sql.includes('AND hash = $2')) {
        return exactMatch
          ? { rowCount: 1, rows: [{ id: 'exact-memory', ...exactMatch, row_version: 'revision-1' }] }
          : { rowCount: 0, rows: [] };
      }

      if (sql.includes('FROM memories AS m')) {
        if (!hasSubjectMatch) {
          return { rowCount: 0, rows: [] };
        }

        return {
          rowCount: 1,
          rows: [{
            account_id: 'account-1',
            id: 'memory-1',
            data: 'Existing fact',
            confidence: 1,
            score: 8,
            salience: 0.8,
            type: 'user_preference',
            scope: matchScope,
            polarity: 'neutral',
            status: matchStatus,
            volatility: 'low',
            evidence: matchEvidence,
            row_version: 'revision-1',
            encrypted_dek: null,
            vault_encryption_enabled: false,
            similarity
          }]
        };
      }

      if (sql.includes('RETURNING id')) {
        return { rowCount: 1, rows: [{ id: 'inserted-memory' }] };
      }

      return { rowCount: 1, rows: [] };
    })
  };
}

describe('deduplicateMemory precomputed conflict decisions', () => {
  beforeEach(() => {
    effects.length = 0;
    decryptForVaultMock.mockReset();
    decryptForVaultMock.mockResolvedValue('Existing fact');
    enforceMemoryCreationLimitMock.mockReset();
    extractorMock.arbitrateConflict.mockReset();
    recordMemoryCountDeltaMock.mockReset();
  });

  it('uses a precomputed decision when it matches the current best memory', async () => {
    const db = createDb();

    await apply(input(), db, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'memory-1',
      precomputedConflictMemoryRevision: 'revision-1'
    });

    expect(extractorMock.arbitrateConflict).not.toHaveBeenCalled();
    expect(db.query.mock.calls.some(([sql]) => String(sql).includes('SET data = $2'))).toBe(true);
  });

  it('never calls live arbitration in a transaction when the precomputed memory no longer matches', async () => {
    const db = createDb();
    extractorMock.arbitrateConflict.mockResolvedValue('discard_new');

    await apply(input(), db, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'different-memory',
      precomputedConflictMemoryRevision: 'revision-1'
    });

    expect(extractorMock.arbitrateConflict).not.toHaveBeenCalled();
  });

  it('does not reuse a precomputed decision after the same memory revision changes', async () => {
    const db = createDb();
    extractorMock.arbitrateConflict.mockResolvedValue('discard_new');

    await apply(input(), db, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'memory-1',
      precomputedConflictMemoryRevision: 'stale-revision'
    });

    expect(extractorMock.arbitrateConflict).not.toHaveBeenCalled();
    expect(db.query.mock.calls.some(([sql]) => String(sql).includes('SET data = $2'))).toBe(false);
  });

  it('limits subject similarity lookup to the best matching memory', async () => {
    const db = createDb();

    await apply(input(), db, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'memory-1',
      precomputedConflictMemoryRevision: 'revision-1'
    });

    const similarityQuery = db.query.mock.calls
      .map(([sql]) => String(sql))
      .find((sql) => sql.includes('FROM memories AS m'));

    expect(similarityQuery).toMatch(/ORDER BY similarity DESC\s+LIMIT 1/);
    expect(similarityQuery).toContain('m.valid_from IS NULL');
    expect(similarityQuery).toContain('m.valid_until IS NULL');
    expect(similarityQuery).toContain('m.valid_until IS NULL OR $5::date IS NULL');
    expect(similarityQuery).toContain('$6::date IS NULL OR m.valid_from IS NULL');
    expect(similarityQuery).toContain("m.status = 'active'");
  });

  it('excludes out-of-window exact matches before automatic consolidation', async () => {
    const db = createDb({ exactMatch: { scope: 'project', status: 'active' } });

    await apply(input(), db);

    const exactQuery = db.query.mock.calls.find(([sql]) => String(sql).includes('hash = $2'));
    expect(String(exactQuery?.[0])).toContain('memories.valid_from IS NULL');
    expect(String(exactQuery?.[0])).toContain('memories.valid_until IS NULL');
    expect(String(exactQuery?.[0])).toContain('memories.valid_until IS NULL OR $4::date IS NULL');
    expect(String(exactQuery?.[0])).toContain('$5::date IS NULL OR memories.valid_from IS NULL');
    expect(String(exactQuery?.[0])).toContain("status = 'active'");
    expect(exactQuery?.[1]?.[2]).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });

  it('does not publish a memory count delta from an uncommitted inner insert', async () => {
    const db = createDb({ hasSubjectMatch: false });

    await apply(input(), db);

    expect(recordMemoryCountDeltaMock).not.toHaveBeenCalled();

    const insertCallIndex = db.query.mock.calls.findIndex(([sql]) => String(sql).includes('INSERT INTO memories'));
    expect(insertCallIndex).toBeGreaterThanOrEqual(0);
  });

  it('keeps the narrower existing scope when an exact match proposes widening', async () => {
    const db = createDb({ exactMatch: { scope: 'session', status: 'active' } });

    await apply({ ...input(), scope: 'global' }, db);

    const update = db.query.mock.calls.find(([sql]) => String(sql).includes('SET source_chunks'));
    expect(update).toBeDefined();
    expect(update?.[1]?.[6]).toBe('global');
    expect(String(update?.[0])).toContain("status = 'active'");
    expect(String(update?.[0])).toContain('archived_at IS NULL');
    expect(String(update?.[0])).toContain("evidence -> 'policy_rejections'");
    expect(String(update?.[0])).toContain('xmin::text = $18');
    expect(String(update?.[0])).toContain('scope AS previous_scope');
    expect(String(update?.[0])).toContain('scope = target.previous_scope');
    expect(String(update?.[0])).toContain('scope_key IS NOT DISTINCT FROM $16::text');
    expect(String(update?.[0])).toContain('valid_from AS previous_valid_from');
    expect(String(update?.[0])).toContain('GREATEST(target.previous_valid_from, $11::date)');
    expect(String(update?.[0])).toContain('LEAST(target.previous_valid_until, $12::date)');
    expect(String(update?.[0])).toContain('INSERT INTO memory_scope_change_log');
    expect(String(update?.[0])).toContain('INSERT INTO memory_authority_events');
    expect(String(update?.[0])).toContain('AND vault_id = $14');
  });

  it('requires the locked row to retain the same binding during an automatic high-similarity merge', async () => {
    const db = createDb({ matchScope: 'project', similarity: 0.95 });

    await apply({ ...input(), scope: 'session', scopeKey: 'session-1' }, db);

    const update = db.query.mock.calls.find(([sql]) => String(sql).includes('SET data = $2'));
    expect(update).toBeDefined();
    expect(update?.[1]?.[9]).toBe('session');
    expect(String(update?.[0])).toContain("status = 'active'");
    expect(String(update?.[0])).toContain('archived_at IS NULL');
    expect(String(update?.[0])).toContain("evidence -> 'policy_rejections'");
    expect(String(update?.[0])).toContain('xmin::text = $21');
    expect(String(update?.[0])).toContain('scope AS previous_scope');
    expect(String(update?.[0])).toContain('scope = target.previous_scope');
    expect(String(update?.[0])).toContain('scope_key IS NOT DISTINCT FROM $19::text');
    expect(String(update?.[0])).toContain('GREATEST(target.previous_valid_from, $14::date)');
    expect(String(update?.[0])).toContain('LEAST(target.previous_valid_until, $15::date)');
    expect(String(update?.[0])).toContain('INSERT INTO memory_scope_change_log');
    expect(String(update?.[0])).toContain("THEN 'proposed' ELSE authority_state END");
    expect(String(update?.[0])).toContain('THEN NULL ELSE approved_by END');
    expect(String(update?.[0])).toContain('AND vault_id = $17');
    expect(String(update?.[0])).toContain("'invalidate'");
  });

  it('derives conflict-merge scope from the row locked by the update', async () => {
    const db = createDb({ matchScope: 'session', similarity: 0.82 });

    await apply({ ...input(), scope: 'global' }, db, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'memory-1',
      precomputedConflictMemoryRevision: 'revision-1'
    });

    const update = db.query.mock.calls.find(([sql]) => String(sql).includes('SET data = $2'));
    expect(update).toBeDefined();
    expect(update?.[1]?.[9]).toBe('global');
    expect(String(update?.[0])).toContain('scope AS previous_scope');
    expect(String(update?.[0])).toContain('scope = target.previous_scope');
    expect(String(update?.[0])).toContain('scope_key IS NOT DISTINCT FROM $19::text');
    expect(String(update?.[0])).toContain('GREATEST(target.previous_valid_from, $14::date)');
    expect(String(update?.[0])).toContain('LEAST(target.previous_valid_until, $15::date)');
    expect(String(update?.[0])).toContain('INSERT INTO memory_scope_change_log');
    expect(String(update?.[0])).toContain('AND vault_id = $17');
  });

  it('requires a still-active eligible row at the write-side lock', async () => {
    const db = createDb({ matchScope: 'session', matchStatus: 'needs_review', similarity: 0.95 });

    await apply({ ...input(), scope: 'session', scopeKey: 'session-1' }, db);

    const update = db.query.mock.calls.find(([sql]) => String(sql).includes('SET data = $2'));
    expect(update).toBeDefined();
    expect(String(update?.[0])).toContain("status = 'active'");
    expect(String(update?.[0])).toContain('archived_at IS NULL');
    expect(String(update?.[0])).toContain("sensitivity <> 'restricted'");
    expect(String(update?.[0])).toContain("evidence -> 'policy_rejections'");
  });

  it('does not request conflict arbitration for quarantined input', async () => {
    const db = createDb();

    const request = await getDedupEscalationRequest({
      ...input(),
      status: 'needs_review',
      policyRejections: [{
        code: 'invalid_memory_scope',
        field: 'scope',
        reason: 'unsupported'
      }]
    }, 'quarantined-memory', db);

    expect(request).toBeNull();
    expect(db.query).not.toHaveBeenCalled();
    expect(decryptForVaultMock).not.toHaveBeenCalled();
    expect(extractorMock.arbitrateConflict).not.toHaveBeenCalled();
  });

  it('preserves policy rejection evidence when an exact-match update remains quarantined', async () => {
    const db = createDb({
      exactMatch: {
        scope: 'session',
        status: 'needs_review',
        evidence: {
          summary: null,
          policy_rejections: [{
            code: 'invalid_memory_scope',
            field: 'scope',
            reason: 'unsupported'
          }]
        }
      }
    });

    await apply({ ...input(), scope: 'session', scopeKey: 'session-1', evidence: 'Later supporting evidence.' }, db);

    const update = db.query.mock.calls.find(([sql]) => String(sql).includes('SET source_chunks'));
    const evidence = JSON.parse(String(update?.[1]?.[9]));
    expect(evidence.summary).toBe('Later supporting evidence.');
    expect(evidence.policy_rejections).toEqual([{
      code: 'invalid_memory_scope',
      field: 'scope',
      reason: 'unsupported'
    }]);
  });

  it('preserves policy rejection evidence across automatic similarity merges', async () => {
    const db = createDb({
      matchScope: 'session',
      matchStatus: 'needs_review',
      matchEvidence: {
        summary: 'Original evidence.',
        policy_rejections: [{
          code: 'global_behavioral_memory_requires_approval',
          field: 'scope',
          reason: 'approval_missing'
        }]
      },
      similarity: 0.95
    });

    await apply({ ...input(), scope: 'session', scopeKey: 'session-1', evidence: 'Updated evidence.' }, db);

    const update = db.query.mock.calls.find(([sql]) => String(sql).includes('SET data = $2'));
    const evidence = JSON.parse(String(update?.[1]?.[12]));
    expect(evidence.summary).toBe('Updated evidence.');
    expect(evidence.policy_rejections).toEqual([{
      code: 'global_behavioral_memory_requires_approval',
      field: 'scope',
      reason: 'approval_missing'
    }]);
  });

  it('inserts quarantined input without mutating a matching active memory', async () => {
    const db = createDb({ exactMatch: { scope: 'global', status: 'active' } });

    await apply({
      ...input(),
      scope: 'session',
      status: 'needs_review',
      policyRejections: [{
        code: 'invalid_memory_scope',
        field: 'scope',
        reason: 'unsupported'
      }]
    }, db);

    const insert = db.query.mock.calls.find(([sql]) => String(sql).includes('INSERT INTO memories'));
    expect(insert).toBeDefined();
    expect(insert?.[1]?.[12]).toBe('session');
    expect(insert?.[1]?.[15]).toBe('needs_review');
    expect(insert?.[1]?.[17]).toContain('invalid_memory_scope');
    expect(insert?.[1]?.[17]).toContain('unsupported');
    expect(db.query.mock.calls.some(([sql]) => String(sql).includes('UPDATE memories'))).toBe(false);
  });

  it('forces policy-rejected input into quarantine even when its caller requests active', async () => {
    const db = createDb({ exactMatch: { scope: 'global', status: 'active' } });

    await apply({
      ...input(),
      status: 'active',
      policyRejections: [{
        code: 'untrusted_provenance',
        field: 'provenance',
        reason: 'imported'
      }]
    }, db);

    const insert = db.query.mock.calls.find(([sql]) => String(sql).includes('INSERT INTO memories'));
    expect(insert?.[1]?.[15]).toBe('needs_review');
    expect(db.query.mock.calls.some(([sql]) => String(sql).includes('UPDATE memories'))).toBe(false);
  });

  it('rejects secret-like content before any dedup read or write', async () => {
    const db = createDb();

    await expect(apply({
      ...input(),
      fact: 'api_key=sk-example-secret-value-123456789'
    }, db)).rejects.toThrow('rejected by secret policy');
    expect(db.query).not.toHaveBeenCalled();
  });
});
