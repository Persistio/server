import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import type { DedupInput, DedupResult, DedupOptions } from '../dedup';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('deduplicateMemory scope concurrency (PostgreSQL)', () => {
  const testPool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  const memoryId = crypto.randomUUID();
  const sourceChunkId = crypto.randomUUID();
  const fact = 'User prefers batched escalation.';
  let embedding: number[] = [];
  let closeDefaultPool = async () => {};
  let migrationAdvisoryLockId = '';
  let runMigrations = async () => {};
  let deduplicateMemory: (
    input: DedupInput,
    db?: never,
    options?: DedupOptions
  ) => Promise<DedupResult>;

  beforeAll(async () => {
    const migration = await testPool.query<{ applied: boolean }>(
      `SELECT EXISTS (
         SELECT 1
         FROM schema_migrations
         WHERE filename = '047_memory_applicability.sql'
       ) AS applied`
    );
    if (!migration.rows[0]?.applied) {
      throw new Error('PERSISTIO_TEST_DATABASE_URL must point to a database migrated through 047_memory_applicability.sql');
    }
    const storageType = await testPool.query<{ vector_type: string }>(
      `SELECT format_type(atttypid, atttypmod) AS vector_type
       FROM pg_attribute
       WHERE attrelid = 'memory_embeddings'::regclass
         AND attname = 'embedding'
         AND NOT attisdropped`
    );
    const dimensions = /^vector\((\d+)\)$/.exec(storageType.rows[0]?.vector_type ?? '')?.[1];
    if (!dimensions) {
      throw new Error('Unable to derive memory embedding dimensions from PostgreSQL');
    }
    embedding = Array.from({ length: Number(dimensions) }, () => 0);
    process.env.DATABASE_URL = databaseUrl;
    process.env.STORAGE_EMBEDDING_DIMENSIONS = dimensions;
    ({
      closePool: closeDefaultPool,
      MIGRATION_ADVISORY_LOCK_ID: migrationAdvisoryLockId,
      runMigrations
    } = await import('../../db/client'));
    const dedup = await import('../dedup');
    const { prepareVaultCrypto } = await import('../crypto');
    const { withTransaction } = await import('../../db/client');
    deduplicateMemory = async (input, interceptor, options = {}) => {
      const bound = { precomputedConflictInput: dedup.fingerprintDedupInput(input), ...options };
      if (!interceptor) return dedup.deduplicateMemory(input, undefined, bound);
      const vault = (await testPool.query('SELECT * FROM vaults WHERE id=$1', [input.vaultId])).rows[0];
      const prepared = await prepareVaultCrypto(vault);
      return withTransaction(async client => {
        const applicationClient = interceptor === testPool ? client : {
          query: (sql: string, values?: unknown[]) => (interceptor as any).query(client, sql, values)
        };
        return dedup.deduplicateMemoryInTransaction(input, applicationClient as never, prepared, [], bound);
      });
    };

    await testPool.query(
      `INSERT INTO vaults (id, name, api_key_hash)
       VALUES ($1, $2, $3)`,
      [vaultId, `scope-concurrency-${vaultId}`, crypto.randomUUID()]
    );
    await testPool.query(
      `INSERT INTO raw_chunks (id, vault_id, session_id, role)
       VALUES ($1, $2, 'scope-concurrency', 'user')`,
      [sourceChunkId, vaultId]
    );
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, polarity, status, volatility
       )
       VALUES ($1, $2, $3, $4, $5, $6::uuid[], 8, 0.8, 'low',
               'user_preference', 'global', 'neutral', 'active', 'low')`,
      [memoryId, vaultId, fact, 'user', crypto.createHash('md5').update(fact).digest('hex'), []]
    );
  });

  afterAll(async () => {
    await testPool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await testPool.end();
    await closeDefaultPool();
  });

  it('rejects omitted and unsupported scope values at the database boundary', async () => {
    await expect(testPool.query(
      `INSERT INTO memories (/* EXPECT_SCOPE_CONSTRAINT_FAILURE */ vault_id, data, subject, hash)
       VALUES ($1, 'fact', 'subject', $2)`,
      [vaultId, crypto.randomUUID()]
    )).rejects.toMatchObject({ code: '23502' });

    await expect(testPool.query(
      `INSERT INTO memories (vault_id, data, subject, hash, scope)
       VALUES ($1, 'fact', 'subject', $2, 'vault')`,
      [vaultId, crypto.randomUUID()]
    )).rejects.toMatchObject({ code: '23514' });
  });

  it('holds worker startup behind the shared migration advisory lock', async () => {
    const blocker = await testPool.connect();
    let migrationCompleted = false;
    await blocker.query('SELECT pg_advisory_lock($1::bigint)', [migrationAdvisoryLockId]);
    const migration = runMigrations().then(() => {
      migrationCompleted = true;
    });

    try {
      let lockWaitObserved = false;
      for (let attempt = 0; attempt < 20 && !lockWaitObserved; attempt += 1) {
        const waiters = await testPool.query<{ count: string }>(
          `SELECT count(*)::text AS count
           FROM pg_locks
           WHERE locktype = 'advisory' AND NOT granted`
        );
        lockWaitObserved = Number(waiters.rows[0]?.count ?? 0) > 0;
        if (!lockWaitObserved) await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(lockWaitObserved).toBe(true);
      expect(migrationCompleted).toBe(false);
    } finally {
      await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [migrationAdvisoryLockId]);
      blocker.release();
    }

    await migration;
    expect(migrationCompleted).toBe(true);
  });

  it('implements the complete least-privilege scope matrix and rejects invalid input', async () => {
    const scopes = ['session', 'task', 'project', 'global'] as const;
    const result = await testPool.query<{
      current_scope: string;
      incoming_scope: string;
      merged_scope: string;
    }>(
      `SELECT current_scope, incoming_scope,
              public.least_privileged_memory_scope(current_scope, incoming_scope) AS merged_scope
       FROM unnest($1::text[]) AS current_scope
       CROSS JOIN unnest($1::text[]) AS incoming_scope`,
      [scopes]
    );

    for (const row of result.rows) {
      expect(row.merged_scope).toBe(scopes[Math.min(
        scopes.indexOf(row.current_scope as typeof scopes[number]),
        scopes.indexOf(row.incoming_scope as typeof scopes[number])
      )]);
    }
    const invalid = await testPool.query<{ merged_scope: string | null }>(
      `SELECT public.least_privileged_memory_scope('session', 'vault') AS merged_scope
       UNION ALL
       SELECT public.least_privileged_memory_scope('session', NULL)`
    );
    expect(invalid.rows).toEqual([{ merged_scope: null }, { merged_scope: null }]);
  });

  it('keeps identical facts in separate scope bindings instead of merging authority', async () => {
    const narrowedMemoryId = crypto.randomUUID();
    const narrowedFact = 'User prefers narrow-scope confirmations.';
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, polarity, status, volatility
       )
       VALUES ($1, $2, $3, 'user', $4, '{}'::uuid[], 8, 0.8, 'low',
               'user_preference', 'global', 'neutral', 'active', 'low')`,
      [narrowedMemoryId, vaultId, narrowedFact, crypto.createHash('md5').update(narrowedFact).digest('hex')]
    );

    await deduplicateMemory({
      vaultId,
      fact: narrowedFact,
      score: 8,
      subject: 'User',
      embedding,
      sourceChunks: [sourceChunkId],
      salience: 0.8,
      sensitivity: 'low',
      type: 'user_preference',
      scope: 'task',
      scopeKey: 'task-1',
      polarity: 'neutral',
      status: 'active',
      volatility: 'low',
      evidence: null,
      validFrom: null,
      validUntil: null,
      sourceSegmentId: null
    }, testPool as never);

    const memories = await testPool.query<{ scope: string; scope_key: string | null }>(
      `SELECT scope, scope_key FROM memories WHERE vault_id = $1 AND hash = $2 ORDER BY scope`,
      [vaultId, crypto.createHash('md5').update(narrowedFact).digest('hex')]
    );
    expect(memories.rows).toEqual([
      { scope: 'global', scope_key: null },
      { scope: 'task', scope_key: 'task-1' }
    ]);
  });

  it('cannot widen a row narrowed after the worker read its stale exact match', async () => {
    let staleMatchObserved = false;
    const racingDb = {
      query: async (client: import('pg').PoolClient, sql: string, values?: unknown[]) => {
        const result = await client.query(sql, values);
        if (!staleMatchObserved && sql.includes('AND hash = $2')) {
          expect(result.rows[0]?.scope).toBe('global');
          staleMatchObserved = true;
          await testPool.query(
            `WITH updated AS (
               UPDATE memories
               SET scope = 'session', scope_key = 'session-raced', updated_at = now()
               WHERE id = $1 AND vault_id = $2
               RETURNING id
             )
             INSERT INTO memory_scope_change_log (
               vault_id, memory_id, old_scope, new_scope, old_scope_key, new_scope_key, actor_type, source, reason
             )
             SELECT $2, id, 'global', 'session', NULL, 'session-raced', 'system', 'system',
                    'Concurrency regression test narrowed scope after the worker read the row.'
             FROM updated`,
            [memoryId, vaultId]
          );
        }
        return result;
      }
    };
    const input: DedupInput = {
      vaultId,
      fact,
      score: 8,
      subject: 'User',
      embedding,
      sourceChunks: [sourceChunkId],
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

    const result = await deduplicateMemory(input, racingDb as never);

    expect(staleMatchObserved).toBe(true);
    expect(result.action).toBe('inserted');
    const memory = await testPool.query<{ scope: string }>(
      'SELECT scope FROM memories WHERE id = $1',
      [memoryId]
    );
    expect(memory.rows[0]?.scope).toBe('session');

    const audit = await testPool.query<{ new_scope: string; old_scope: string }>(
      `SELECT old_scope, new_scope
       FROM memory_scope_change_log
       WHERE vault_id = $1 AND memory_id = $2
       ORDER BY created_at`,
      [vaultId, memoryId]
    );
    expect(audit.rows).toEqual([{ old_scope: 'global', new_scope: 'session' }]);
  });

  it('intersects validity windows under lock instead of extending existing authority', async () => {
    const validityMemoryId = crypto.randomUUID();
    const validityFact = 'The maintenance window is active in June 2026.';
    const dateOffset = (days: number) => new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
    const existingFrom = dateOffset(-30);
    const existingUntil = dateOffset(30);
    const narrowerFrom = dateOffset(-15);
    const narrowerUntil = dateOffset(15);
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, scope_key, polarity, status, volatility, valid_from, valid_until
       )
       VALUES ($1, $2, $3, 'maintenance', $4, '{}'::uuid[], 8, 0.8, 'low',
               'system_fact', 'project', 'project-1', 'neutral', 'active', 'low', $5::date, $6::date)`,
      [
        validityMemoryId,
        vaultId,
        validityFact,
        crypto.createHash('md5').update(validityFact).digest('hex'),
        existingFrom,
        existingUntil
      ]
    );

    const merge = async (validFrom: string | null, validUntil: string | null) => deduplicateMemory({
      vaultId,
      fact: validityFact,
      score: 8,
      subject: 'maintenance',
      embedding,
      sourceChunks: [sourceChunkId],
      salience: 0.8,
      sensitivity: 'low',
      type: 'system_fact',
      scope: 'project',
      scopeKey: 'project-1',
      polarity: 'neutral',
      status: 'active',
      volatility: 'low',
      evidence: null,
      validFrom,
      validUntil,
      sourceSegmentId: null
    }, testPool as never);

    await merge(dateOffset(-60), dateOffset(60));
    await merge(narrowerFrom, narrowerUntil);

    const memory = await testPool.query<{ valid_from: string; valid_until: string }>(
      `SELECT valid_from::text, valid_until::text
       FROM memories
       WHERE id = $1`,
      [validityMemoryId]
    );
    expect(memory.rows).toEqual([{ valid_from: narrowerFrom, valid_until: narrowerUntil }]);
  });

  it('does not let an expired exact match absorb a newly extracted memory', async () => {
    const historicalId = crypto.randomUUID();
    const historicalFact = 'The temporary rollout flag is enabled.';
    const historicalHash = crypto.createHash('md5').update(historicalFact).digest('hex');
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, scope_key, polarity, status, volatility, valid_until
       )
       VALUES ($1, $2, $3, 'rollout', $4, '{}'::uuid[], 8, 0.8, 'low',
               'system_fact', 'project', 'project-1', 'neutral', 'active', 'low',
               (now() AT TIME ZONE 'UTC')::date - 1)`,
      [historicalId, vaultId, historicalFact, historicalHash]
    );

    const result = await deduplicateMemory({
      vaultId,
      fact: historicalFact,
      score: 8,
      subject: 'rollout',
      embedding,
      sourceChunks: [sourceChunkId],
      salience: 0.8,
      sensitivity: 'low',
      type: 'system_fact',
      scope: 'project',
      scopeKey: 'project-1',
      polarity: 'neutral',
      status: 'active',
      volatility: 'low',
      evidence: null,
      validFrom: null,
      validUntil: null,
      sourceSegmentId: null
    }, testPool as never);

    expect(result.action).toBe('inserted');
    const rows = await testPool.query<{ id: string; valid_until: string | null }>(
      `SELECT id::text, valid_until::text
       FROM memories
       WHERE vault_id = $1 AND hash = $2
       ORDER BY created_at, id`,
      [vaultId, historicalHash]
    );
    expect(rows.rows).toHaveLength(2);
    expect(rows.rows.find((row) => row.id === historicalId)?.valid_until).not.toBeNull();
    expect(rows.rows.find((row) => row.id === result.memoryId)?.valid_until).toBeNull();
  });

  it('cannot widen a validity window narrowed after the worker read its stale exact match', async () => {
    const validityMemoryId = crypto.randomUUID();
    const validityFact = 'The incident change window remains active.';
    const dateOffset = (days: number) => new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
    const narrowedFrom = dateOffset(-10);
    const narrowedUntil = dateOffset(10);
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, scope_key, polarity, status, volatility, valid_from, valid_until
       )
       VALUES ($1, $2, $3, 'incident', $4, '{}'::uuid[], 8, 0.8, 'low',
               'system_fact', 'project', 'project-1', 'neutral', 'active', 'low', $5::date, $6::date)`,
      [
        validityMemoryId,
        vaultId,
        validityFact,
        crypto.createHash('md5').update(validityFact).digest('hex'),
        dateOffset(-30),
        dateOffset(30)
      ]
    );

    let staleMatchObserved = false;
    const racingDb = {
      query: async (client: import('pg').PoolClient, sql: string, values?: unknown[]) => {
        const result = await client.query(sql, values);
        if (!staleMatchObserved && sql.includes('AND hash = $2')) {
          expect(result.rows[0]?.id).toBe(validityMemoryId);
          staleMatchObserved = true;
          await testPool.query(
            `UPDATE memories
             SET valid_from = $2::date, valid_until = $3::date
             WHERE id = $1`,
            [validityMemoryId, narrowedFrom, narrowedUntil]
          );
        }
        return result;
      }
    };

    await deduplicateMemory({
      vaultId,
      fact: validityFact,
      score: 8,
      subject: 'incident',
      embedding,
      sourceChunks: [sourceChunkId],
      salience: 0.8,
      sensitivity: 'low',
      type: 'system_fact',
      scope: 'project',
      scopeKey: 'project-1',
      polarity: 'neutral',
      status: 'active',
      volatility: 'low',
      evidence: null,
      validFrom: dateOffset(-60),
      validUntil: dateOffset(60),
      sourceSegmentId: null
    }, racingDb as never);

    expect(staleMatchObserved).toBe(true);
    const memory = await testPool.query<{ valid_from: string; valid_until: string }>(
      `SELECT valid_from::text, valid_until::text
       FROM memories
       WHERE id = $1`,
      [validityMemoryId]
    );
    expect(memory.rows).toEqual([{ valid_from: narrowedFrom, valid_until: narrowedUntil }]);
  });

  it.each([
    ['needs-review', 'needs_review', false],
    ['superseded and archived', 'superseded', true]
  ])('does not consolidate into a %s row', async (_label, status, archived) => {
    const originalId = crypto.randomUUID();
    const isolatedFact = `Inactive consolidation boundary ${originalId}`;
    const original = await testPool.query<{ id: string }>(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, polarity, status, volatility, archived_at
       ) VALUES ($1, $2, $3, 'inactive-boundary', $4, '{}'::uuid[], 8, 0.8,
                 'low', 'system_fact', 'global', 'neutral', $5, 'low',
                 CASE WHEN $6::boolean THEN now() ELSE NULL END)
       RETURNING id`,
      [
        originalId,
        vaultId,
        isolatedFact,
        crypto.createHash('md5').update(isolatedFact).digest('hex'),
        status,
        archived
      ]
    );

    const result = await deduplicateMemory({
      vaultId,
      fact: isolatedFact,
      score: 8,
      subject: 'inactive-boundary',
      embedding,
      sourceChunks: [sourceChunkId],
      salience: 0.8,
      sensitivity: 'low',
      type: 'system_fact',
      scope: 'global',
      scopeKey: null,
      polarity: 'neutral',
      status: 'active',
      volatility: 'low',
      evidence: null,
      validFrom: null,
      validUntil: null,
      sourceSegmentId: null
    }, testPool as never);

    expect(result.action).toBe('inserted');
    expect(result.memoryId).not.toBe(original.rows[0].id);
    expect((await testPool.query<{ archived: boolean; status: string }>(
      `SELECT status, archived_at IS NOT NULL AS archived FROM memories WHERE id = $1`,
      [originalId]
    )).rows[0]).toEqual({ status, archived });
  });

  it('invalidates prior authority when exact dedup attaches new provenance', async () => {
    const approvedId = crypto.randomUUID();
    const approvedFact = `Approved provenance boundary ${approvedId}`;
    const newChunkId = crypto.randomUUID();
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, source_chunks, score, salience,
         sensitivity, type, scope, polarity, status, volatility
       ) VALUES ($1, $2, $3, 'authority-boundary', $4, '{}'::uuid[], 8, 0.8,
                 'low', 'system_fact', 'global', 'neutral', 'active', 'low')`,
      [approvedId, vaultId, approvedFact, crypto.createHash('md5').update(approvedFact).digest('hex')]
    );
    await testPool.query(
      `UPDATE memories
       SET authority_state = 'approved', approved_by = 'integration-test', approved_at = now(), approval_source = 'test'
       WHERE id = $1`,
      [approvedId]
    );
    await testPool.query(
      `INSERT INTO raw_chunks (id, vault_id, session_id, role)
       VALUES ($1, $2, 'authority-boundary', 'user')`,
      [newChunkId, vaultId]
    );

    await deduplicateMemory({
      vaultId,
      fact: approvedFact,
      score: 8,
      subject: 'authority-boundary',
      embedding,
      sourceChunks: [newChunkId],
      salience: 0.8,
      sensitivity: 'low',
      type: 'system_fact',
      scope: 'global',
      scopeKey: null,
      polarity: 'neutral',
      status: 'active',
      volatility: 'low',
      evidence: 'New source evidence.',
      validFrom: null,
      validUntil: null,
      sourceSegmentId: null
    }, testPool as never);

    const updated = (await testPool.query<{
      approved_by: string | null;
      authority_state: string;
      authority_version: number;
      source_chunks: string[];
    }>(
      `SELECT approved_by, authority_state, authority_version, source_chunks
       FROM memories WHERE id = $1`,
      [approvedId]
    )).rows[0];
    expect(updated.authority_state).toBe('proposed');
    expect(updated.approved_by).toBeNull();
    expect(updated.authority_version).toBeGreaterThan(1);
    expect(updated.source_chunks).toContain(newChunkId);
  });

  it('rolls back an arbitrated retirement when inserting its replacement fails', async () => {
    const existingId = crypto.randomUUID();
    const existingEmbedding = [1, ...Array.from({ length: embedding.length - 1 }, () => 0)];
    const incomingEmbedding = [0.85, Math.sqrt(1 - (0.85 ** 2)), ...Array.from({ length: embedding.length - 2 }, () => 0)];
    await testPool.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, embedding, source_chunks, score, salience,
         sensitivity, type, scope, polarity, status, volatility
       ) VALUES ($1, $2, 'Old transaction fact.', 'transaction-subject', $3, $4::vector,
                 '{}'::uuid[], 8, 0.8, 'low', 'user_preference', 'global', 'neutral', 'active', 'low')`,
      [existingId, vaultId, crypto.randomUUID(), JSON.stringify(existingEmbedding)]
    );
    await testPool.query(
      `INSERT INTO memory_embeddings (memory_id, embedding) VALUES ($1, $2::vector)`,
      [existingId, JSON.stringify(existingEmbedding)]
    );
    const existingRevision = (await testPool.query<{ row_version: string }>(
      `SELECT xmin::text AS row_version FROM memories WHERE id = $1`,
      [existingId]
    )).rows[0].row_version;

    await expect(deduplicateMemory({
      vaultId,
      fact: 'New transaction fact.',
      score: 8,
      subject: 'transaction-subject',
      embedding: incomingEmbedding,
      sourceChunks: ['not-a-uuid'],
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
    }, undefined, {
      precomputedConflictDecision: 'supersede_old',
      precomputedConflictMemoryId: existingId,
      precomputedConflictMemoryRevision: existingRevision
    })).rejects.toThrow();

    expect((await testPool.query<{ status: string }>(
      `SELECT status FROM memories WHERE id = $1`, [existingId]
    )).rows[0].status).toBe('active');
  });
});
