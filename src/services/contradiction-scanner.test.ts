import { beforeEach, describe, expect, it, vi } from 'vitest';

const { clientQueryMock, decryptForVaultMock, policyCounterAddMock, queryMock, withTransactionMock } = vi.hoisted(() => ({
  clientQueryMock: vi.fn(),
  decryptForVaultMock: vi.fn(async (_vault, value: string) => value),
  policyCounterAddMock: vi.fn(),
  queryMock: vi.fn(),
  withTransactionMock: vi.fn()
}));

vi.mock('../metrics', () => ({
  memoryPolicyEventCounter: { add: policyCounterAddMock }
}));

vi.mock('../config', () => ({
  getConfig: () => ({
    CONTRADICTION_SCAN_ENABLED: true,
    CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH: 4,
    CONTRADICTION_SCAN_MIN_SIMILARITY: 0.8,
    GLOBAL_RULE_POLICY: 'approved_only'
  })
}));

vi.mock('../db/client', () => ({
  query: queryMock,
  withTransaction: withTransactionMock
}));

vi.mock('./crypto', () => ({
  decryptForVault: decryptForVaultMock,
  MemoryCiphertextError: class extends Error {}
}));

import { scanForContradictions } from './contradiction-scanner';
import { MemoryCiphertextError } from './crypto';

const activeMemory = {
  memory_id: '11111111-1111-4111-8111-111111111111',
  data: 'Current fact',
  polarity: 'neutral',
  status: 'active',
  similarity: 1,
  scope: 'project',
  scope_key: 'project-1',
  row_version: 'revision-current',
  authority_eligible: true,
  source_timestamp: '2026-08-01T00:00:00Z',
  valid_from: '2026-08-01',
  valid_until: null,
  created_at: '2026-08-01T00:00:00Z',
  id: 'vault-1',
  encrypted_dek: null,
  vault_encryption_enabled: false
};

describe('scanForContradictions quarantine boundaries', () => {
  beforeEach(() => {
    clientQueryMock.mockReset();
    clientQueryMock.mockImplementation(async (sql, params) => ({
      rowCount: String(sql).startsWith('SELECT') ? 2 : Array.isArray(params?.[1]) ? params[1].length : 1, rows: []
    }));
    decryptForVaultMock.mockReset();
    decryptForVaultMock.mockImplementation(async (_vault, data) => data);
    queryMock.mockReset();
    queryMock.mockImplementation(async sql => ({ rowCount: String(sql).startsWith('UPDATE') ? 1 : 2, rows: [] }));
    withTransactionMock.mockReset();
    withTransactionMock.mockImplementation(async (run) => run({ query: clientQueryMock }));
    policyCounterAddMock.mockReset();
  });

  function selectCandidates(texts = ['candidate', 'later']) {
    const candidates = texts.map((data, index) => ({ ...activeMemory, data,
      memory_id: `22222222-2222-4222-8222-22222222222${index}`,
      row_version: `candidate-${index}`, similarity: 0.9 }));
    queryMock.mockResolvedValueOnce({ rows: [activeMemory] })
      .mockResolvedValueOnce({ rows: candidates });
    return candidates;
  }

  it.each(['discard_new', 'merge', 'needs_review'])('stops after terminal %s', async decision => {
    selectCandidates();
    const extractor = { arbitrateConflict: vi.fn(async () => decision) };
    expect((await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never)).completedMemoryIds)
      .toEqual([activeMemory.memory_id]);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(1);
    expect(withTransactionMock).toHaveBeenCalledTimes(1);
  });

  it.each(['off', 'approved_only', 'legacy'] as const)('keeps supplied %s policy through selection and commit', async policy => {
    selectCandidates(['candidate']);
    await scanForContradictions('vault-1', [activeMemory.memory_id], {
      arbitrateConflict: vi.fn(async () => 'discard_new')
    } as never, { globalRulePolicy: policy });
    expect(queryMock.mock.calls[0][1][2]).toBe(policy);
    expect(queryMock.mock.calls[1][1][4]).toBe(policy);
    const lockedInputs = clientQueryMock.mock.calls.find(([sql]) => String(sql).includes('FOR UPDATE OF m'));
    expect(lockedInputs?.[1][5]).toBe(policy);
  });

  it.each(['supersede_old', 'discard_new', 'merge', 'needs_review'])('rechecks applicability after locking before %s', async decision => {
    selectCandidates(['candidate']);
    clientQueryMock.mockResolvedValueOnce({ rowCount: null, rows: [] }) // Isolation setting.
      .mockResolvedValueOnce({ rowCount: 2, rows: [] }) // Both original revisions locked.
      .mockResolvedValueOnce({ rowCount: 1, rows: [] }); // Fresh authority/date check fails.
    await expect(scanForContradictions('vault-1', [activeMemory.memory_id], {
      arbitrateConflict: vi.fn(async () => decision)
    } as never)).rejects.toThrow('inputs changed after arbitration');
    const statements = clientQueryMock.mock.calls.map(([sql]) => String(sql));
    expect(statements[0]).toBe('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
    expect(statements[1]).toContain('ORDER BY m.id FOR UPDATE OF m');
    expect(statements[2]).toContain("(statement_timestamp() AT TIME ZONE 'UTC')::date");
    expect(statements[2]).toContain("source_timestamp <= statement_timestamp() + interval '5 minutes'");
    expect(statements[2]).toContain('memory_authority_events');
    expect(statements.some(sql => sql.startsWith('UPDATE') || sql.startsWith('INSERT'))).toBe(false);
  });

  const unequalHorizons = [
    { label: 'unbounded versus bounded start', first: [null, null], second: ['2026-08-01', null] },
    { label: 'unbounded versus bounded end', first: ['2026-08-01', null], second: ['2026-08-01', '2026-10-01'] },
    { label: 'partial overlap', first: ['2026-08-01', '2026-10-01'], second: ['2026-08-15', '2026-11-01'] },
    { label: 'nested finite intervals', first: ['2026-08-01', '2026-11-01'], second: ['2026-08-15', '2026-10-01'] }
  ];
  it.each(unequalHorizons.flatMap(horizon => [false, true].flatMap(reverse => [false, true].map(exactText => ({
    ...horizon, reverse, exactText
  })))))('preserves $label for review (reverse=$reverse, exact=$exactText)', async ({ first, second, reverse, exactText }) => {
    const [currentBounds, candidateBounds] = reverse ? [second, first] : [first, second];
    const current = { ...activeMemory, valid_from: currentBounds[0], valid_until: currentBounds[1] };
    const candidate = { ...activeMemory, memory_id: '22222222-2222-4222-8222-222222222222',
      row_version: 'candidate-revision', data: exactText ? current.data : 'Different fact',
      valid_from: candidateBounds[0], valid_until: candidateBounds[1] };
    queryMock.mockResolvedValueOnce({ rows: [current] }).mockResolvedValueOnce({ rows: [candidate] });
    const budget = { remaining: 2 };
    const extractor = { arbitrateConflict: vi.fn(async () => 'merge') };

    const result = await scanForContradictions('vault-1', [current.memory_id], extractor as never, { budget });

    expect(result).toEqual({ completedMemoryIds: [current.memory_id], deferredMemoryIds: [] });
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(budget.remaining).toBe(2);
    const writes = clientQueryMock.mock.calls.filter(([sql]) => String(sql).startsWith('UPDATE memories'));
    expect(writes).toHaveLength(1);
    expect(writes[0][1]).toEqual(['vault-1', [current.memory_id, candidate.memory_id], 'needs_review']);
    expect(writes[0][0]).not.toMatch(/SET confidence|SET valid_/);
    const log = clientQueryMock.mock.calls.find(([sql]) => String(sql).includes('INSERT INTO contradiction_scan_log'));
    expect(log?.[1][3]).toBe('needs_review');
  });

  it('retains cap-incomplete work and counts failed calls against the shared budget', async () => {
    selectCandidates();
    const budget = { remaining: 1 };
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    expect((await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never, { budget })).deferredMemoryIds)
      .toEqual([activeMemory.memory_id]);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(1);
    expect(budget.remaining).toBe(0);
    selectCandidates();
    const failedBudget = { remaining: 1 };
    await expect(scanForContradictions('vault-1', [activeMemory.memory_id], {
      arbitrateConflict: async () => { throw new Error('provider outage'); }
    } as never, { budget: failedBudget })).rejects.toThrow('provider outage');
    expect(failedBudget.remaining).toBe(0);
  });

  it('isolates a corrupt candidate and still examines the next good candidate', async () => {
    const candidates = selectCandidates(['corrupt', 'good']);
    decryptForVaultMock.mockImplementation(async (_vault, text) => {
      if (text === 'corrupt') throw new MemoryCiphertextError('invalid payload');
      return text;
    });
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    expect((await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never)).deferredMemoryIds)
      .toEqual([activeMemory.memory_id]);
    expect(extractor.arbitrateConflict).toHaveBeenCalledWith('good', 'Current fact', 'vault-1', expect.any(Object));
    const quarantine = queryMock.mock.calls.find(([sql]) => String(sql).startsWith('UPDATE memories'));
    expect(quarantine?.[1]).toEqual(['vault-1', candidates[0].memory_id, 'candidate-0']);
  });

  it('does not quarantine data for a KMS or credential outage', async () => {
    selectCandidates();
    decryptForVaultMock.mockRejectedValue(new Error('KMS unavailable'));
    await expect(scanForContradictions('vault-1', [activeMemory.memory_id], {} as never)).rejects.toThrow('KMS unavailable');
    expect(queryMock.mock.calls.some(([sql]) => String(sql).startsWith('UPDATE memories'))).toBe(false);
  });

  it.each([0, 1])('distinguishes an empty search from a concurrent metadata update (%i)', async rowCount => {
    selectCandidates([]);
    queryMock.mockResolvedValueOnce({ rowCount, rows: [] });
    const result = await scanForContradictions('vault-1', [activeMemory.memory_id], {} as never);
    expect(rowCount ? result.completedMemoryIds : result.deferredMemoryIds).toEqual([activeMemory.memory_id]);
  });

  it('defers an unapproved memory without decrypting or calling the model', async () => {
    queryMock.mockResolvedValueOnce({ rows: [{ ...activeMemory, authority_eligible: false }] });
    expect((await scanForContradictions('vault-1', [activeMemory.memory_id], {} as never)).deferredMemoryIds)
      .toEqual([activeMemory.memory_id]);
    expect(decryptForVaultMock).not.toHaveBeenCalled();
  });

  it('supplies neutral temporal context when an older reminder compares with newer evidence', async () => {
    const newer = { ...activeMemory, memory_id: '22222222-2222-4222-8222-222222222222',
      data: 'newer fact', row_version: 'newer', source_timestamp: '2026-09-01T00:00:00Z',
      created_at: '2026-09-01T00:00:00Z' };
    queryMock.mockResolvedValueOnce({ rows: [activeMemory] }).mockResolvedValueOnce({ rows: [newer] });
    const extractor = { arbitrateConflict: vi.fn(async () => 'discard_new') };
    await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledWith('newer fact', 'Current fact', 'vault-1', {
      existing: { sourceTimestamp: newer.source_timestamp, validFrom: newer.valid_from,
        validUntil: null, createdAt: newer.created_at },
      incoming: { sourceTimestamp: activeMemory.source_timestamp, validFrom: activeMemory.valid_from,
        validUntil: null, createdAt: activeMemory.created_at }
    });
    expect(clientQueryMock.mock.calls.find(([sql]) => String(sql).startsWith('UPDATE memories'))?.[1])
      .toEqual(['vault-1', [activeMemory.memory_id], 'contradicted']);
  });

  it('does not decrypt or arbitrate a non-active current memory', async () => {
    queryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ ...activeMemory, status: 'needs_review' }]
    });
    const extractor = { arbitrateConflict: vi.fn() };

    await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never);

    expect(String(queryMock.mock.calls[0][0])).toContain("m.status = 'active'");
    expect(String(queryMock.mock.calls[0][0])).toContain('m.valid_from IS NULL');
    expect(String(queryMock.mock.calls[0][0])).toContain('m.valid_until IS NULL');
    expect(String(queryMock.mock.calls[0][0])).toContain("m.evidence -> 'policy_rejections'");
    expect(queryMock).toHaveBeenCalledTimes(1);
    expect(decryptForVaultMock).not.toHaveBeenCalled();
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
  });

  it('does not decrypt or arbitrate non-active candidate matches', async () => {
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [activeMemory] })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{
          ...activeMemory,
          memory_id: '22222222-2222-4222-8222-222222222222',
          data: 'Quarantined fact',
          status: 'needs_review',
          row_version: 'revision-candidate',
          similarity: 0.9
        }]
      });
    const extractor = { arbitrateConflict: vi.fn() };

    await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never);

    expect(String(queryMock.mock.calls[1][0])).toContain("current.status = 'active'");
    expect(String(queryMock.mock.calls[1][0])).toContain("m.status = 'active'");
    expect(String(queryMock.mock.calls[1][0])).toContain('m.scope = current.scope');
    expect(String(queryMock.mock.calls[1][0])).toContain('m.scope_key IS NOT DISTINCT FROM current.scope_key');
    expect(String(queryMock.mock.calls[1][0])).toContain('current.valid_from IS NULL');
    expect(String(queryMock.mock.calls[1][0])).toContain('current.valid_until IS NULL');
    expect(String(queryMock.mock.calls[1][0])).toContain('m.valid_from IS NULL');
    expect(String(queryMock.mock.calls[1][0])).toContain('m.valid_until IS NULL');
    expect(String(queryMock.mock.calls[1][0])).toContain('current.xmin::text = $4');
    expect(decryptForVaultMock).toHaveBeenCalledTimes(1);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(withTransactionMock).not.toHaveBeenCalled();
  });

  it('guards contradiction mutations against a row becoming non-active after selection', async () => {
    const candidate = {
      ...activeMemory,
      memory_id: '22222222-2222-4222-8222-222222222222',
      data: 'Conflicting fact',
      row_version: 'revision-candidate',
      similarity: 0.9
    };
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [activeMemory] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [candidate] });
    const extractor = { arbitrateConflict: vi.fn(async () => 'discard_new') };

    await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never);

    const updateSql = clientQueryMock.mock.calls
      .map(([sql]) => String(sql))
      .filter((sql) => sql.includes('UPDATE memories'));
    expect(updateSql).toHaveLength(1);
    const lockedCall = clientQueryMock.mock.calls.find(([sql]) => String(sql).includes('FOR UPDATE OF m'))!;
    const lockedSql = String(lockedCall[0]);
    expect(lockedSql).toContain("status = 'active'");
    expect(lockedSql).toContain('ORDER BY m.id FOR UPDATE OF m');
    expect(lockedCall[1]).toEqual([
      'vault-1',
      activeMemory.memory_id,
      candidate.memory_id,
      'revision-current',
      'revision-candidate',
      'approved_only'
    ]);
    expect(lockedSql).toContain('m.xmin::text = $4');
    expect(lockedSql).toContain('m.valid_from IS NULL');
    expect(lockedSql).toContain('m.valid_until IS NULL');
    expect(lockedSql).toContain('memory_authority_events');
  });

  it('keeps merged confidence inside the recall eligibility range', async () => {
    const candidate = {
      ...activeMemory,
      memory_id: '22222222-2222-4222-8222-222222222222',
      data: 'Compatible fact',
      row_version: 'revision-candidate',
      similarity: 0.9
    };
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [activeMemory] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [candidate] });
    const extractor = { arbitrateConflict: vi.fn(async () => 'merge') };

    await scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never);

    const confidenceUpdate = clientQueryMock.mock.calls
      .map(([sql]) => String(sql))
      .find((sql) => sql.includes('SET confidence'));
    expect(confidenceUpdate).toContain('LEAST(confidence + 0.1, 1)');
  });

  it('does not mutate when arbitration returns an unknown value', async () => {
    const candidate = {
      ...activeMemory,
      memory_id: '22222222-2222-4222-8222-222222222222',
      data: 'Conflicting fact',
      row_version: 'revision-candidate',
      similarity: 0.9
    };
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [activeMemory] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [candidate] });
    const extractor = { arbitrateConflict: vi.fn(async () => 'keep_both') };

    await expect(scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never))
      .rejects.toThrow('Invalid contradiction arbitration decision');
    expect(withTransactionMock).not.toHaveBeenCalled();
    expect(clientQueryMock).not.toHaveBeenCalled();
  });

  it('rolls back the decision when a selected target changes before mutation', async () => {
    const candidate = {
      ...activeMemory,
      memory_id: '22222222-2222-4222-8222-222222222222',
      data: 'Conflicting fact',
      row_version: 'revision-candidate',
      similarity: 0.9
    };
    queryMock
      .mockResolvedValueOnce({ rowCount: 1, rows: [activeMemory] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [candidate] });
    clientQueryMock.mockResolvedValueOnce({ rowCount: null, rows: [] }) // Isolation setting.
      .mockResolvedValueOnce({ rowCount: 0, rows: [] });
    const extractor = { arbitrateConflict: vi.fn(async () => 'discard_new') };

    await expect(scanForContradictions('vault-1', [activeMemory.memory_id], extractor as never))
      .rejects.toThrow('inputs changed after arbitration');
    expect(clientQueryMock).toHaveBeenCalledTimes(2);
  });
});
