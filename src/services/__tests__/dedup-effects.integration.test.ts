import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import type { DedupInput } from '../dedup';
const events = vi.hoisted(() => ({ metric: vi.fn() }));
vi.mock('../customer-metrics', async original => ({ ...(await original() as typeof import('../customer-metrics')), recordCustomerMetric: events.metric }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('dedup commit-owned effects (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  let db: typeof import('../../db/client'), dedup: typeof import('../dedup'), crypt: typeof import('../crypto');
  let vector: number[];
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    db = await import('../../db/client'); await db.runMigrations();
    const config = (await import('../../config')).getConfig(); config.ENCRYPTION_ENABLED = false;
    vector = [1, ...Array(config.STORAGE_EMBEDDING_DIMENSIONS - 1).fill(0)];
    dedup = await import('../dedup'); crypt = await import('../crypto');
  });
  afterEach(async () => {
    vi.restoreAllMocks(); events.metric.mockReset();
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]); vaults.length = 0;
  });
  afterAll(async () => { await pool.end(); await db?.closePool(); });
  async function fixture(accountless = false) {
    const vaultId = crypto.randomUUID(), account = accountless ? null : crypto.randomUUID(); vaults.push(vaultId);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,account_id,plan_id) VALUES ($1,'dedup-effects',$2,$3,'unlimited')", [vaultId, crypto.randomUUID(), account]);
    const input: DedupInput = { vaultId, fact: 'Durable storage uses PostgreSQL.', subject: 'storage', score: 9, salience: 0.9,
      embedding: vector, sourceChunks: [], sensitivity: 'low', type: 'domain_knowledge', scope: 'global', scopeKey: null,
      polarity: 'neutral', status: 'active', volatility: 'low', validFrom: null, validUntil: null };
    return { input, account };
  }
  const deltas = () => events.metric.mock.calls.map(([event]) => event).filter(event => event.event_type === 'quota_delta');
  async function usage(vaultId: string) { return (await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1', [vaultId])).rows[0]?.memory_adds ?? 0; }

  it.each(['candidate', 'needs_review', 'active', 'keep_both', 'conflict'] as const)('emits both dimensions after the %s insertion commits', async branch => {
    const { input, account } = await fixture();
    if (branch === 'candidate' || branch === 'needs_review') input.status = branch;
    if (branch === 'keep_both' || branch === 'conflict') {
      await dedup.deduplicateMemory(input);
      events.metric.mockClear();
      input.fact = 'A distinct related storage fact.';
      input.embedding = [0.82, Math.sqrt(1-0.82**2), ...vector.slice(2)];
      if (branch === 'conflict') input.type = 'user_preference';
    }
    const before = await usage(input.vaultId);
    const visible: Promise<unknown>[] = [];
    events.metric.mockImplementation(event => {
      if (event.event_type === 'quota_delta') visible.push(usage(input.vaultId).then(count => expect(count).toBe(before+1)));
    });
    const result = await dedup.deduplicateMemory(input); await Promise.all(visible);
    expect(result.action).toBe('inserted');
    const memory = (await pool.query('SELECT status FROM memories WHERE id=$1', [result.memoryId])).rows[0];
    expect(memory.status).toBe(branch === 'conflict' ? 'needs_review' : branch === 'keep_both' ? 'active' : branch);
    expect(deltas()).toEqual([
      expect.objectContaining({ operation: 'memory_adds', memory_adds_delta: 1, workspace_id: account, source: 'extraction_worker' }),
      expect.objectContaining({ operation: 'memory_count', memory_count_delta: 1, workspace_id: account, source: 'extraction_worker' })
    ]);
  });
  it('does not create another addition on an exact merge, and accountless vaults still charge SQL', async () => {
    const { input } = await fixture(true);
    await dedup.deduplicateMemory(input); await dedup.deduplicateMemory(input);
    expect(await usage(input.vaultId)).toBe(1);
    expect(deltas()).toEqual([]);
  });
  it('discards quota and count effects when embedding SQL fails', async () => {
    const { input } = await fixture(); input.embedding = [1, 2];
    await expect(dedup.deduplicateMemory(input)).rejects.toThrow();
    expect(await usage(input.vaultId)).toBe(0);
    expect((await pool.query('SELECT 1 FROM memories WHERE vault_id=$1', [input.vaultId])).rowCount).toBe(0);
    expect(deltas()).toEqual([]);
  });
  it('does not guess events or refund when COMMIT acknowledgement is lost', async () => {
    const { input } = await fixture();
    const transaction = db.withTransaction;
    vi.spyOn(db, 'withTransaction').mockImplementationOnce(async work => {
      await transaction(work); // Server committed, but the caller receives no confirmation.
      throw new Error('connection lost receiving COMMIT');
    });
    await expect(dedup.deduplicateMemory(input)).rejects.toThrow('connection lost');
    expect(await usage(input.vaultId)).toBe(1);
    expect((await pool.query('SELECT 1 FROM memories WHERE vault_id=$1', [input.vaultId])).rowCount).toBe(1);
    expect(deltas()).toEqual([]);
  });
  it('preflights arbitration without locks and safely refuses a decision for a changed input', async () => {
    const { input } = await fixture(); input.type = 'user_preference';
    await dedup.deduplicateMemory(input);
    input.fact = 'Incoming preference.'; input.embedding = [0.85, Math.sqrt(1-0.85**2), ...vector.slice(2)];
    const provider = { arbitrateConflict: vi.fn(async () => {
      const probe = await pool.connect();
      try { await probe.query('BEGIN'); await probe.query('SELECT * FROM memories WHERE vault_id=$1 FOR UPDATE NOWAIT', [input.vaultId]); }
      finally { await probe.query('ROLLBACK'); probe.release(); }
      input.fact = 'Changed after preflight';
      return 'discard_new';
    }) };
    await dedup.deduplicateMemory(input, provider as never);
    expect(provider.arbitrateConflict).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT status FROM memories WHERE vault_id=$1', [input.vaultId])).rows).toEqual([{ status: 'needs_review' }, { status: 'needs_review' }]);
  });
  it('applies two prepared candidates targeting one revision without a live provider fallback', async () => {
    const { input } = await fixture(); input.type = 'user_preference'; await dedup.deduplicateMemory(input);
    const first = { ...input, fact: 'First new preference', embedding: [0.85, Math.sqrt(1-0.85**2), ...vector.slice(2)] };
    const y = (0.85 - 0.85**2) / Math.sqrt(1-0.85**2);
    const second = { ...first, fact: 'Second new preference',
      embedding: [0.85, y, Math.sqrt(1-0.85**2-y**2), ...vector.slice(3)] };
    const requests = await Promise.all([first, second].map((candidate, i) => dedup.getDedupEscalationRequest(candidate, String(i))));
    const vault = (await pool.query('SELECT * FROM vaults WHERE id=$1', [input.vaultId])).rows[0];
    const prepared = await crypt.prepareVaultCrypto(vault);
    await db.withTransaction(async client => {
      for (const [index, candidate] of [first, second].entries()) {
        const request = requests[index]!;
        await dedup.deduplicateMemoryInTransaction(candidate, client, prepared, [], {
          precomputedConflictDecision: 'merge', precomputedConflictMemoryId: request.existingMemoryId,
          precomputedConflictMemoryRevision: request.existingMemoryRevision, precomputedConflictInput: request.inputFingerprint
        });
      }
    });
    expect(await usage(input.vaultId)).toBe(2);
    expect((await pool.query('SELECT status FROM memories WHERE vault_id=$1', [input.vaultId])).rows)
      .toEqual([{ status: 'needs_review' }, { status: 'needs_review' }]);
  });
  it('uses only its supplied client for canonical lookup in the locked phase', async () => {
    const { input } = await fixture();
    const prepared = await crypt.prepareVaultCrypto((await pool.query('SELECT * FROM vaults WHERE id=$1', [input.vaultId])).rows[0]);
    const globalQuery = vi.spyOn(db, 'query').mockRejectedValue(new Error('Second pool connection inside apply'));
    await db.withTransaction(client => dedup.deduplicateMemoryInTransaction(input, client, prepared, []));
    expect(globalQuery).not.toHaveBeenCalled();
  });
});
