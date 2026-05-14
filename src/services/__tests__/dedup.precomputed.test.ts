import { beforeEach, describe, expect, it, vi } from 'vitest';

const { decryptForVaultMock, enforceMemoryCreationLimitMock, extractorMock } = vi.hoisted(() => ({
  decryptForVaultMock: vi.fn(),
  enforceMemoryCreationLimitMock: vi.fn(),
  extractorMock: {
    arbitrateConflict: vi.fn()
  }
}));

vi.mock('../crypto', () => ({
  computeSubjectHmac: vi.fn(() => 'subject-hmac'),
  decryptForVault: decryptForVaultMock,
  encryptForVault: vi.fn(async (_vault, value: string) => value),
  encryptSubjectForVault: vi.fn(async () => null),
  isVaultEncryptionActive: vi.fn(() => false)
}));

vi.mock('../entity-resolver', () => ({
  normaliseSubject: vi.fn((subject: string) => subject.toLowerCase().trim()),
  resolveCanonical: vi.fn(async () => null)
}));

vi.mock('../usage', () => ({
  enforceMemoryCreationLimit: enforceMemoryCreationLimitMock
}));

vi.mock('../../telemetry', () => ({
  withSpan: async (_name: string, _attributes: Record<string, unknown>, fn: (span: {
    setAttribute: (key: string, value: string | number | boolean) => void;
  }) => Promise<unknown>) => fn({ setAttribute: vi.fn() })
}));

import { deduplicateMemory, type DedupInput } from '../dedup';

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
    polarity: 'neutral',
    status: 'active',
    volatility: 'low',
    evidence: null,
    validFrom: null,
    validUntil: null,
    sourceSegmentId: null
  };
}

function createDb() {
  return {
    query: vi.fn(async (sql: string) => {
      if (sql.includes('FROM vaults')) {
        return {
          rowCount: 1,
          rows: [{ id: 'vault-1', encrypted_dek: null, vault_encryption_enabled: false }]
        };
      }

      if (sql.includes('WHERE vault_id = $1 AND hash = $2')) {
        return { rowCount: 0, rows: [] };
      }

      if (sql.includes('FROM memories AS m')) {
        return {
          rowCount: 1,
          rows: [{
            id: 'memory-1',
            data: 'Existing fact',
            confidence: 1,
            score: 8,
            salience: 0.8,
            type: 'user_preference',
            polarity: 'neutral',
            status: 'active',
            volatility: 'low',
            encrypted_dek: null,
            vault_encryption_enabled: false,
            similarity: 0.82
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
    decryptForVaultMock.mockReset();
    decryptForVaultMock.mockResolvedValue('Existing fact');
    enforceMemoryCreationLimitMock.mockReset();
    extractorMock.arbitrateConflict.mockReset();
  });

  it('uses a precomputed decision when it matches the current best memory', async () => {
    const db = createDb();

    await deduplicateMemory(input(), db, extractorMock as never, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'memory-1'
    });

    expect(extractorMock.arbitrateConflict).not.toHaveBeenCalled();
    expect(db.query.mock.calls.some(([sql]) => String(sql).includes('SET data = $2'))).toBe(true);
  });

  it('falls back to live arbitration when the precomputed memory no longer matches', async () => {
    const db = createDb();
    extractorMock.arbitrateConflict.mockResolvedValue('discard_new');

    await deduplicateMemory(input(), db, extractorMock as never, {
      precomputedConflictDecision: 'merge',
      precomputedConflictMemoryId: 'different-memory'
    });

    expect(extractorMock.arbitrateConflict).toHaveBeenCalledWith(
      'Existing fact',
      'User prefers batched escalation.',
      'vault-1'
    );
  });
});
