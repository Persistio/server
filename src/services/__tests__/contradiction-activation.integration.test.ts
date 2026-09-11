import crypto from 'node:crypto';
import fs from 'node:fs';

import { Client, Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);
type Policy = 'off' | 'approved_only' | 'legacy';

describeWithPostgres('contradiction activation lifecycle (PostgreSQL)', () => {
  const testPool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  let drain: typeof import('../contradiction-activation').drainDueContradictionActivations;
  let scan: typeof import('../contradiction-scanner').scanForContradictions;
  let config: ReturnType<typeof import('../../config').getConfig>;
  let applicationQuery: typeof import('../../db/client').query;
  let closeDefaultPool = async () => {};
  let vector = '';

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    process.env.DB_POOL_MAX = '1';
    config = (await import('../../config')).getConfig();
    const db = await import('../../db/client');
    ({ closePool: closeDefaultPool, query: applicationQuery } = db);
    await db.runMigrations();
    ({ drainDueContradictionActivations: drain } = await import('../contradiction-activation'));
    ({ scanForContradictions: scan } = await import('../contradiction-scanner'));
    const type = await testPool.query<{ type: string }>(
      `SELECT format_type(atttypid, atttypmod) AS type FROM pg_attribute
       WHERE attrelid = 'memories'::regclass AND attname = 'embedding'`
    );
    const dimensions = Number(/\((\d+)\)/.exec(type.rows[0].type)?.[1]);
    vector = JSON.stringify(Array.from({ length: dimensions }, (_, i) => i === 0 ? 1 : 0));
  });

  beforeEach(() => {
    config.CONTRADICTION_SCAN_ENABLED = true;
    config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH = 4;
    config.GLOBAL_RULE_POLICY = 'approved_only';
  });

  afterAll(async () => {
    await testPool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [vaults]);
    await testPool.end();
    await closeDefaultPool();
  });

  async function newVault(): Promise<string> {
    const id = crypto.randomUUID();
    vaults.push(id);
    await testPool.query('INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)',
      [id, `activation-${id}`, crypto.randomUUID()]);
    return id;
  }

  async function addMemory(vaultId: string, data: string, options: {
    approve?: boolean; validFrom?: string | null; validUntil?: string | null; status?: string;
    scope?: 'project' | 'global'; type?: string;
  } = {}): Promise<string> {
    const id = crypto.randomUUID();
    await testPool.query(
      `INSERT INTO memories (id, vault_id, data, subject, hash, embedding, scope, scope_key,
         status, valid_from, valid_until, type)
       VALUES ($1, $2, $3, 'activation', $4, $5::vector, $6, $7, $8, $9, $10, $11)`,
      [id, vaultId, data, crypto.randomUUID(), vector, options.scope ?? 'project',
        options.scope === 'global' ? null : 'activation', options.status ?? 'active',
        options.validFrom ?? null, options.validUntil ?? null, options.type ?? null]
    );
    if (options.approve !== false) await approveMemory(vaultId, id);
    return id;
  }

  async function approveMemory(vaultId: string, id: string): Promise<void> {
    await testPool.query("UPDATE memories SET authority_state = 'approved' WHERE id = $1", [id]);
    await testPool.query(
      `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
         new_version, actor_type, source, reason)
       SELECT $1, id, 'approve', 'approved', authority_version, 'user', 'api', 'integration approval'
       FROM memories WHERE id = $2`, [vaultId, id]
    );
  }

  async function onlySchedule(vaultId: string, id: string): Promise<void> {
    await testPool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id = $1 AND memory_id <> $2', [vaultId, id]);
  }

  async function dueNow(vaultId: string, policy: Policy = config.GLOBAL_RULE_POLICY): Promise<void> {
    await testPool.query("UPDATE memory_contradiction_schedule SET available_at = now() - interval '1 second' WHERE vault_id = $1 AND policy = $2", [vaultId, policy]);
    await testPool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at = now() - interval '1 second' WHERE vault_id = $1 AND policy = $2", [vaultId, policy]);
  }

  async function clearSchedules(): Promise<void> {
    await testPool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id = ANY($1::uuid[])', [vaults]);
  }

  async function addLegacyProof(vaultId: string, memoryId: string): Promise<void> {
    await testPool.query(
      `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
         new_version, actor_type, source, reason, snapshot)
       SELECT $1, id, 'migration', 'proposed', authority_version, 'system', 'migration',
              'legacy scheduling fixture', jsonb_build_object('type', type, 'scope', scope,
                'status', status, 'archived_at', archived_at)
       FROM memories WHERE id = $2`, [vaultId, memoryId]
    );
  }

  async function addPolicyBlockedRules(vaultId: string, count: number, policy: 'off' | 'approved_only'): Promise<void> {
    const inserted = await testPool.query<{ id: string }>(
      `INSERT INTO memories (vault_id, data, subject, hash, scope, scope_key, status, type)
       SELECT $1, 'Policy blocked rule ' || n, 'policy-blocked', gen_random_uuid()::text,
              'global', NULL, 'active', 'user_rule' FROM generate_series(1, $2::int) n
       RETURNING id`, [vaultId, count]
    );
    const ids = inserted.rows.map(row => row.id);
    if (policy === 'off') {
      await testPool.query("UPDATE memories SET authority_state = 'approved' WHERE id = ANY($1::uuid[])", [ids]);
    }
    await testPool.query(
      `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
         new_version, actor_type, source, reason, snapshot)
       SELECT vault_id, id, $2, authority_state, authority_version, 'system', $3,
              'blocked policy fixture', jsonb_build_object('type', type, 'scope', scope,
                'status', status, 'archived_at', archived_at)
       FROM memories WHERE id = ANY($1::uuid[])`,
      [ids, policy === 'off' ? 'approve' : 'migration', policy === 'off' ? 'api' : 'migration']
    );
  }

  async function scheduleState(memoryId: string) {
    return (await testPool.query<{
      policy: Policy; generation: string; failures: number; available_at: Date; authority_ready: boolean;
    }>(
      `SELECT policy, generation, failures, available_at, authority_ready
       FROM memory_contradiction_schedule WHERE memory_id = $1 ORDER BY policy`, [memoryId]
    )).rows;
  }

  async function registryState(vaultId: string) {
    return (await testPool.query<{ policy: Policy; pending_count: number; revision: string; next_visit_at: Date }>(
      `SELECT policy, pending_count::int, revision::text, next_visit_at
       FROM memory_contradiction_pending_vaults WHERE vault_id = $1 ORDER BY policy`, [vaultId]
    )).rows;
  }

  it('persists a future reminder across idle ticks and scans when its date opens', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const today = (await testPool.query<{ today: string }>(
      `SELECT ((now() AT TIME ZONE 'UTC')::date)::text AS today`
    )).rows[0].today;
    const old = await addMemory(vaultId, 'User lives in London.', { validFrom: today });
    const next = await addMemory(vaultId, 'User lives in Paris.', { validFrom: '2099-01-01' });
    await onlySchedule(vaultId, next);
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect((await testPool.query("SELECT memory_id FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [next])).rowCount).toBe(1);
    await testPool.query('UPDATE memories SET valid_from = (now() AT TIME ZONE \'UTC\')::date WHERE id = $1', [next]);
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect((await testPool.query('SELECT status FROM memories WHERE id = $1', [old])).rows[0].status).toBe('contradicted');
    expect((await testPool.query("SELECT memory_id FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [next])).rowCount).toBe(0);
  });

  it('scans a database-due activation when the application clock is a day behind', async () => {
    await clearSchedules();
    const dates = (await testPool.query<{ today: string; yesterday: string }>(
      `SELECT ((now() AT TIME ZONE 'UTC')::date)::text AS today,
              ((now() AT TIME ZONE 'UTC')::date - 1)::text AS yesterday`
    )).rows[0];
    const vaultId = await newVault();
    const old = await addMemory(vaultId, 'Previous valid fact.', { validFrom: dates.today });
    const current = await addMemory(vaultId, 'Fact becoming valid today.', { validFrom: dates.today });
    await onlySchedule(vaultId, current);
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };

    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date(`${dates.yesterday}T12:00:00.000Z`));
    try {
      await drain(extractor as never);
    } finally { vi.useRealTimers(); }

    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect((await testPool.query('SELECT status FROM memories WHERE id = $1', [old])).rows[0].status).toBe('contradicted');
    expect((await testPool.query('SELECT 1 FROM contradiction_scan_log WHERE vault_id = $1', [vaultId])).rowCount).toBe(1);
    expect((await testPool.query("SELECT 1 FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [current])).rowCount).toBe(0);
  });

  it('keeps a database-future activation queued when the application clock is a day ahead', async () => {
    await clearSchedules();
    const tomorrow = (await testPool.query<{ tomorrow: string }>(
      `SELECT ((now() AT TIME ZONE 'UTC')::date + 1)::text AS tomorrow`
    )).rows[0].tomorrow;
    const vaultId = await newVault();
    const old = await addMemory(vaultId, 'Currently valid fact.');
    const future = await addMemory(vaultId, 'Fact becoming valid tomorrow.', { validFrom: tomorrow });
    await onlySchedule(vaultId, future);
    const generation = (await testPool.query("SELECT generation FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [future])).rows[0].generation;
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };

    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date(`${tomorrow}T12:00:00.000Z`));
    try {
      await drain(extractor as never);
    } finally { vi.useRealTimers(); }

    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect((await testPool.query('SELECT status FROM memories WHERE id = ANY($1::uuid[])', [[old, future]])).rows.map(row => row.status))
      .toEqual(['active', 'active']);
    expect((await testPool.query('SELECT 1 FROM contradiction_scan_log WHERE vault_id = $1', [vaultId])).rowCount).toBe(0);
    expect((await testPool.query("SELECT generation, available_at > now() AS waiting FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [future])).rows[0])
      .toEqual({ generation, waiting: true });
  });

  it('retains disabled and unapproved work, then runs after approval with a one-connection application pool', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    await addMemory(vaultId, 'Old fact.');
    const current = await addMemory(vaultId, 'New fact.', { approve: false });
    await onlySchedule(vaultId, current);
    const extractor = { arbitrateConflict: vi.fn(async () => {
      expect((await applicationQuery('SELECT 1 AS ok')).rows[0].ok).toBe(1);
      return 'supersede_old';
    }) };
    config.CONTRADICTION_SCAN_ENABLED = false;
    await drain(extractor as never);
    config.CONTRADICTION_SCAN_ENABLED = true;
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    await approveMemory(vaultId, current);
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });

  it('runs an approved activation ahead of older authority holds and refreshes priority on approval', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const held: string[] = [];
    for (let i = 0; i < 40; i += 1) {
      held.push(await addMemory(vaultId, `Unapproved held fact ${i}.`, { approve: false }));
    }
    const old = await addMemory(vaultId, 'Previously approved fact.');
    const current = await addMemory(vaultId, 'New approved fact.');
    await testPool.query('DELETE FROM memory_contradiction_schedule WHERE memory_id = $1', [old]);
    const beforeApproval = (await testPool.query(
      "SELECT generation, authority_ready FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [held[0]]
    )).rows[0];
    expect(beforeApproval.authority_ready).toBe(false);

    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(extractor.arbitrateConflict.mock.calls[0].slice(0, 2)).toEqual(['Previously approved fact.', 'New approved fact.']);
    expect((await testPool.query("SELECT 1 FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [current])).rowCount).toBe(0);
    expect((await testPool.query("SELECT count(*)::int AS count FROM memory_contradiction_schedule WHERE vault_id = $1 AND policy = 'approved_only'", [vaultId])).rows[0].count).toBe(40);

    await approveMemory(vaultId, held[0]);
    const afterApproval = (await testPool.query(
      "SELECT generation, authority_ready FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [held[0]]
    )).rows[0];
    expect(afterApproval.authority_ready).toBe(true);
    expect(afterApproval.generation).not.toBe(beforeApproval.generation);
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(2);
    expect(extractor.arbitrateConflict.mock.calls[1].slice(0, 2)).toEqual(['New approved fact.', 'Unapproved held fact 0.']);
  });

  it('runs an authorized legacy proposal ahead of ordinary unapproved global rules', async () => {
    await clearSchedules();
    config.GLOBAL_RULE_POLICY = 'legacy';
    const vaultId = await newVault();
    const held: string[] = [];
    for (let i = 0; i < 40; i += 1) {
      held.push(await addMemory(vaultId, `Unapproved global rule ${i}.`, {
        approve: false, scope: 'global', type: 'user_rule'
      }));
    }
    const old = await addMemory(vaultId, 'Previously approved global rule.', { scope: 'global', type: 'user_rule' });
    const legacy = await addMemory(vaultId, 'Authorized migrated global rule.', {
      approve: false, scope: 'global', type: 'user_rule'
    });
    await testPool.query(
      `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
         new_version, actor_type, source, reason, snapshot)
       SELECT $1, id, 'migration', 'proposed', authority_version, 'system', 'migration',
              'legacy migration fixture', jsonb_build_object('type', type, 'scope', scope,
                'status', status, 'archived_at', archived_at)
       FROM memories WHERE id = $2`, [vaultId, legacy]
    );
    // Historical migration events precede 053. This ordinary metadata change
    // exercises the same priority calculation used during upgrade backfill.
    await testPool.query('UPDATE memories SET confidence = 0.9 WHERE id = $1', [legacy]);
    await testPool.query('DELETE FROM memory_contradiction_schedule WHERE memory_id = $1', [old]);
    expect((await testPool.query("SELECT authority_ready FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'legacy'", [legacy])).rows[0].authority_ready)
      .toBe(true);
    expect((await testPool.query("SELECT count(*)::int AS count FROM memory_contradiction_schedule WHERE memory_id = ANY($1::uuid[]) AND policy = 'legacy' AND NOT authority_ready", [held])).rows[0].count)
      .toBe(40);

    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(extractor.arbitrateConflict.mock.calls[0].slice(0, 2))
      .toEqual(['Previously approved global rule.', 'Authorized migrated global rule.']);
    expect((await testPool.query("SELECT 1 FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'legacy'", [legacy])).rowCount).toBe(0);
  });

  it('keeps unmatched or invalid legacy migration evidence in the authority-waiting priority', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const variants = ['version', 'type', 'scope', 'status', 'archived', 'source', 'state'] as const;
    for (const variant of variants) {
      const id = await addMemory(vaultId, `Invalid legacy ${variant}.`, {
        approve: false, scope: 'global', type: 'user_rule'
      });
      const snapshot = {
        type: variant === 'type' ? 'fact' : 'user_rule',
        scope: variant === 'scope' ? 'project' : 'global',
        status: variant === 'status' ? 'candidate' : 'active',
        archived_at: variant === 'archived' ? '2026-01-01T00:00:00Z' : null
      };
      await testPool.query(
        `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
           new_version, actor_type, source, reason, snapshot)
         SELECT $1, id, 'migration', $3, authority_version + $4, 'system', $5,
                'invalid legacy priority fixture', $6::jsonb
         FROM memories WHERE id = $2`,
        [vaultId, id, variant === 'state' ? 'approved' : 'proposed', variant === 'version' ? 1 : 0,
          variant === 'source' ? 'api' : 'migration', JSON.stringify(snapshot)]
      );
      expect((await testPool.query(
        "SELECT contradiction_authority_eligible(m, 'legacy') AS ready FROM memories m WHERE id = $1", [id]
      )).rows[0].ready, variant).toBe(false);
    }
  });

  it.each(['off', 'approved_only'] as const)('does not let a policy-blocked backlog delay an eligible memory under %s', async policy => {
    await clearSchedules();
    config.GLOBAL_RULE_POLICY = policy;
    const vaultId = await newVault();
    await addPolicyBlockedRules(vaultId, 100, policy);
    const old = await addMemory(vaultId, 'Earlier approved project fact.');
    const current = await addMemory(vaultId, 'Later approved project fact.');
    await testPool.query('DELETE FROM memory_contradiction_schedule WHERE memory_id = $1', [old]);
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };

    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(extractor.arbitrateConflict.mock.calls[0].slice(0, 2))
      .toEqual(['Earlier approved project fact.', 'Later approved project fact.']);
    expect((await testPool.query(
      'SELECT count(*)::int AS count FROM memory_contradiction_schedule WHERE vault_id = $1 AND policy = $2 AND NOT authority_ready',
      [vaultId, policy]
    )).rows[0].count).toBe(100);
    expect((await scheduleState(current)).some(row => row.policy === policy)).toBe(false);
  });

  it.each(['off', 'approved_only'] as const)('reaches an eligible vault beyond more than twenty policy-blocked vaults under %s', async policy => {
    await clearSchedules();
    config.GLOBAL_RULE_POLICY = policy;
    const blockedVaults: string[] = [];
    for (let i = 0; i < 25; i += 1) {
      const vaultId = await newVault();
      blockedVaults.push(vaultId);
      await addPolicyBlockedRules(vaultId, 1, policy);
    }
    const eligibleVault = await newVault();
    const old = await addMemory(eligibleVault, 'Earlier eligible fact.');
    const current = await addMemory(eligibleVault, 'Later eligible fact.');
    await onlySchedule(eligibleVault, current);
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };

    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(extractor.arbitrateConflict.mock.calls[0][2]).toBe(eligibleVault);
    expect((await testPool.query(
      `SELECT COALESCE(sum(pending_count), 0)::int AS ready
       FROM memory_contradiction_pending_vaults WHERE vault_id = ANY($1::uuid[]) AND policy = $2`,
      [blockedVaults, policy]
    )).rows[0].ready).toBe(0);
    expect((await testPool.query('SELECT status FROM memories WHERE id = $1', [old])).rows[0].status).toBe('contradicted');
  });

  it('exposes the right retained work after policy flips without rewriting memories', async () => {
    await clearSchedules();
    const approvedVault = await newVault();
    await addMemory(approvedVault, 'Old approved global rule.', { scope: 'global', type: 'user_rule' });
    const approved = await addMemory(approvedVault, 'Current approved global rule.', { scope: 'global', type: 'user_rule' });
    await onlySchedule(approvedVault, approved);
    const legacyVault = await newVault();
    await addMemory(legacyVault, 'Old rule for legacy comparison.', { scope: 'global', type: 'user_rule' });
    const legacy = await addMemory(legacyVault, 'Migrated global rule.', { approve: false, scope: 'global', type: 'user_rule' });
    await addLegacyProof(legacyVault, legacy);
    await onlySchedule(legacyVault, legacy);
    const approvedBefore = await scheduleState(approved);
    const legacyBefore = await scheduleState(legacy);
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };

    config.GLOBAL_RULE_POLICY = 'off';
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await scheduleState(approved)).toEqual(approvedBefore);
    expect(await scheduleState(legacy)).toEqual(legacyBefore);
    config.GLOBAL_RULE_POLICY = 'approved_only';
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(await scheduleState(legacy)).toEqual(legacyBefore);
    expect(await scheduleState(approved)).toEqual(approvedBefore.filter(row => row.policy !== 'approved_only'));
    config.GLOBAL_RULE_POLICY = 'legacy';
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(2);
    expect((await scheduleState(legacy)).some(row => row.policy === 'legacy')).toBe(false);
    config.GLOBAL_RULE_POLICY = 'off';
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(2);
    expect((await scheduleState(legacy)).find(row => row.policy === 'off'))
      .toEqual(legacyBefore.find(row => row.policy === 'off'));
  });

  it('isolates retry and completion when workers with different policies alternate', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const rule = await addMemory(vaultId, 'Legacy-only candidate rule.', { approve: false, scope: 'global', type: 'user_rule' });
    await addLegacyProof(vaultId, rule);
    const current = await addMemory(vaultId, 'Approved factual global memory.', { scope: 'global' });
    await onlySchedule(vaultId, current);
    const initial = await scheduleState(current);
    const initialRegistry = await registryState(vaultId);
    config.GLOBAL_RULE_POLICY = 'legacy';
    const failed = { arbitrateConflict: vi.fn(async () => { throw new Error('provider temporarily unavailable'); }) };
    await drain(failed as never);
    expect(failed.arbitrateConflict).toHaveBeenCalledOnce();
    const afterFailure = await scheduleState(current);
    const afterFailureRegistry = await registryState(vaultId);
    expect(afterFailure.filter(row => row.policy !== 'legacy')).toEqual(initial.filter(row => row.policy !== 'legacy'));
    expect(afterFailureRegistry.filter(row => row.policy !== 'legacy')).toEqual(initialRegistry.filter(row => row.policy !== 'legacy'));
    expect(afterFailure.find(row => row.policy === 'legacy')?.failures).toBe(1);
    expect(afterFailure.find(row => row.policy === 'legacy')?.generation).toBe(initial.find(row => row.policy === 'legacy')?.generation);
    const noModel = { arbitrateConflict: vi.fn() };
    config.GLOBAL_RULE_POLICY = 'off';
    await drain(noModel as never);
    expect(await scheduleState(current)).toEqual(afterFailure.filter(row => row.policy !== 'off'));
    expect((await registryState(vaultId)).filter(row => row.policy !== 'off'))
      .toEqual(afterFailureRegistry.filter(row => row.policy !== 'off'));
    config.GLOBAL_RULE_POLICY = 'approved_only';
    await drain(noModel as never);
    expect(noModel.arbitrateConflict).not.toHaveBeenCalled();
    expect(await scheduleState(current)).toEqual(afterFailure.filter(row => row.policy === 'legacy'));
    expect((await registryState(vaultId)).find(row => row.policy === 'legacy'))
      .toEqual(afterFailureRegistry.find(row => row.policy === 'legacy'));
    await dueNow(vaultId, 'legacy');
    config.GLOBAL_RULE_POLICY = 'legacy';
    const retry = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(retry as never);
    expect(retry.arbitrateConflict).toHaveBeenCalledOnce();
    expect(await scheduleState(current)).toEqual([]);
  });

  it('captures each concurrent worker policy and keeps the vault lock shared across policies', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const rule = await addMemory(vaultId, 'Legacy-only comparison rule.', { approve: false, scope: 'global', type: 'user_rule' });
    await addLegacyProof(vaultId, rule);
    const current = await addMemory(vaultId, 'Approved global fact.', { scope: 'global' });
    await onlySchedule(vaultId, current);
    const initial = await scheduleState(current);
    const initialRegistry = await registryState(vaultId);
    let entered = false;
    let release!: () => void;
    const resume = new Promise<void>(resolve => { release = resolve; });
    const legacyExtractor = { arbitrateConflict: vi.fn(async () => {
      entered = true;
      await resume;
      return 'supersede_old';
    }) };
    config.GLOBAL_RULE_POLICY = 'legacy';
    const running = drain(legacyExtractor as never);
    const offExtractor = { arbitrateConflict: vi.fn() };
    try {
      await expect.poll(() => entered, { timeout: 2_000 }).toBe(true);
      config.GLOBAL_RULE_POLICY = 'off';
      await drain(offExtractor as never);
      expect(offExtractor.arbitrateConflict).not.toHaveBeenCalled();
      expect(await scheduleState(current)).toEqual(initial);
      expect((await registryState(vaultId)).filter(row => row.policy !== 'off'))
        .toEqual(initialRegistry.filter(row => row.policy !== 'off'));
    } finally {
      release();
      await running;
    }
    expect(legacyExtractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect((await testPool.query('SELECT status FROM memories WHERE id = $1', [rule])).rows[0].status).toBe('contradicted');
    expect(await scheduleState(current)).toEqual(initial.filter(row => row.policy !== 'legacy'));
    await dueNow(vaultId, 'off');
    await drain(offExtractor as never);
    expect(offExtractor.arbitrateConflict).not.toHaveBeenCalled();
    expect((await scheduleState(current)).map(row => row.policy)).toEqual(['approved_only']);
  });

  const eventOnlyCases = (['current', 'candidate'] as const).flatMap(target =>
    (['supersede_old', 'discard_new', 'merge', 'needs_review'] as const).map(decision => ({ target, decision, serializable: false })));
  eventOnlyCases.push({ target: 'current', decision: 'supersede_old', serializable: true });
  it.each(eventOnlyCases)('rechecks an event-only $target revocation before $decision after waiting for input locks (serializable default: $serializable)', async ({ target, decision, serializable }) => {
    await clearSchedules();
    const vaultId = await newVault();
    const candidate = await addMemory(vaultId, 'Earlier approved global rule.', { scope: 'global', type: 'user_rule' });
    const current = await addMemory(vaultId, 'Later approved global rule.', { scope: 'global', type: 'user_rule' });
    await testPool.query(
      "UPDATE memory_contradiction_schedule SET available_at = now() - interval '1 hour' WHERE memory_id = $1", [current]
    );
    const targetId = target === 'current' ? current : candidate;
    const beforeSchedule = (await scheduleState(targetId)).find(row => row.policy === 'approved_only')!;
    const beforeMemories = (await testPool.query(
      `SELECT id, status, data, confidence, authority_state, authority_version::text, xmin::text AS row_version
       FROM memories WHERE vault_id = $1 ORDER BY id`, [vaultId]
    )).rows;
    const eventClient = await testPool.connect();
    await eventClient.query('BEGIN');
    await eventClient.query('SELECT id FROM memories WHERE id = $1 FOR UPDATE', [targetId]);
    await eventClient.query(
      `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
         new_version, actor_type, source, reason)
       SELECT $1, id, 'revoke', 'revoked', authority_version + 1, 'user', 'api', 'event-only revocation'
       FROM memories WHERE id = $2`, [vaultId, targetId]
    );
    const extractor = { arbitrateConflict: vi.fn(async () => decision) };
    const originalDatabaseUrl = config.DATABASE_URL;
    if (serializable) {
      const connectionUrl = new URL(originalDatabaseUrl);
      connectionUrl.searchParams.set('options', '-c default_transaction_isolation=serializable');
      config.DATABASE_URL = connectionUrl.toString();
    }
    const running = drain(extractor as never);
    let committed = false;
    try {
      await expect.poll(async () => (await testPool.query(
        `SELECT count(*)::int AS waiting FROM pg_stat_activity
         WHERE datname = current_database() AND application_name = 'persistio:contradiction-activation'
           AND wait_event_type = 'Lock'`
      )).rows[0].waiting, { timeout: 2_000 }).toBe(1);
      expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
      await eventClient.query('COMMIT');
      committed = true;
      await running;
    } finally {
      try {
        if (!committed) await eventClient.query('ROLLBACK');
        eventClient.release();
        await running;
      } finally {
        config.DATABASE_URL = originalDatabaseUrl;
      }
    }
    expect((await testPool.query(
      `SELECT id, status, data, confidence, authority_state, authority_version::text, xmin::text AS row_version
       FROM memories WHERE vault_id = $1 ORDER BY id`, [vaultId]
    )).rows).toEqual(beforeMemories);
    expect((await testPool.query('SELECT 1 FROM contradiction_scan_log WHERE vault_id = $1', [vaultId])).rowCount).toBe(0);
    const afterSchedule = (await scheduleState(targetId)).find(row => row.policy === 'approved_only')!;
    expect(afterSchedule).toBeDefined();
    expect(afterSchedule.generation).not.toBe(beforeSchedule.generation);
    expect(afterSchedule.authority_ready).toBe(false);
  });

  it('keeps indexed discovery work bounded as blocked memory and vault backlogs grow', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const current = await addMemory(vaultId, 'Only ready memory.');
    await onlySchedule(vaultId, current);
    const statements = [
      { name: 'memory_contradiction_schedule_ready_due', sql:
        `SELECT memory_id, generation, failures FROM memory_contradiction_schedule
         WHERE vault_id = $1 AND policy = $2 AND authority_ready AND available_at <= now()
         ORDER BY available_at, memory_id LIMIT 1`, values: [vaultId, 'approved_only'] },
      { name: 'memory_contradiction_pending_vaults_due', sql:
        `SELECT vault_id, revision::text FROM memory_contradiction_pending_vaults
         WHERE policy = $1 AND pending_count > 0 AND next_visit_at <= now()
         ORDER BY next_visit_at, vault_id LIMIT $2`, values: ['approved_only', 20] }
    ];
    type Plan = { Plans?: Plan[]; 'Index Name'?: string; 'Node Type': string; 'Actual Rows': number;
      'Rows Removed by Filter'?: number; 'Shared Hit Blocks'?: number; 'Shared Read Blocks'?: number };
    const inspect = async () => {
      const client = await testPool.connect();
      await client.query('BEGIN');
      try {
        // Small fixtures can otherwise choose a sequential scan by cost. Inspect
        // the production index path and actual visited rows/buffers, not SQL text.
        await client.query('SET LOCAL enable_seqscan = off');
        const buffers: number[] = [];
        for (const statement of statements) {
          const result = await client.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) ${statement.sql}`, statement.values);
          const plan = result.rows[0]['QUERY PLAN'][0].Plan as Plan;
          const flatten = (node: Plan): Plan[] => [node, ...(node.Plans ?? []).flatMap(flatten)];
          const nodes = flatten(plan);
          expect(nodes.some(node => node['Index Name'] === statement.name)).toBe(true);
          expect(nodes.some(node => node['Node Type'] === 'Seq Scan')).toBe(false);
          expect(nodes.reduce((sum, node) => sum + (node['Rows Removed by Filter'] ?? 0), 0)).toBeLessThanOrEqual(1);
          expect(nodes.every(node => node['Actual Rows'] <= 20)).toBe(true);
          buffers.push((plan['Shared Hit Blocks'] ?? 0) + (plan['Shared Read Blocks'] ?? 0));
        }
        return buffers;
      } finally { await client.query('ROLLBACK'); client.release(); }
    };
    const before = await inspect();
    await addPolicyBlockedRules(vaultId, 500, 'approved_only');
    for (let i = 0; i < 30; i += 1) await addPolicyBlockedRules(await newVault(), 1, 'approved_only');
    await testPool.query('ANALYZE memory_contradiction_schedule');
    await testPool.query('ANALYZE memory_contradiction_pending_vaults');
    const after = await inspect();
    expect(after[0]).toBeLessThanOrEqual(before[0] + 8);
    expect(after[1]).toBeLessThanOrEqual(before[1] + 8);
  }, 15_000);

  it('resumes a partial scan and preserves one budget across independent vaults', async () => {
    await clearSchedules();
    const first = await newVault();
    const second = await newVault();
    await addMemory(first, 'First old A.');
    await addMemory(first, 'First old B.');
    const current = await addMemory(first, 'First current.');
    await onlySchedule(first, current);
    await addMemory(second, 'Second old.');
    const otherCurrent = await addMemory(second, 'Second current.');
    await onlySchedule(second, otherCurrent);
    await dueNow(first);
    await dueNow(second);
    config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH = 1;
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(2);
    await dueNow(first);
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledTimes(3);
  });

  it('retains provider failures without losing generations and charges the failed call to the tick budget', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    await addMemory(vaultId, 'Old fact.');
    const current = await addMemory(vaultId, 'New fact.');
    await onlySchedule(vaultId, current);
    config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH = 1;
    const before = (await testPool.query("SELECT generation FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [current])).rows[0].generation;
    const extractor = { arbitrateConflict: vi.fn(async () => { throw new Error('provider unavailable'); }) };
    await drain(extractor as never);
    const row = (await testPool.query("SELECT generation, failures, available_at > now() AS delayed FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [current])).rows[0];
    expect(row).toEqual({ generation: before, failures: 1, delayed: true });
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });

  it('skips a locked vault and releases ownership after a completed tick', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    await addMemory(vaultId, 'Old fact.');
    const current = await addMemory(vaultId, 'New fact.');
    await onlySchedule(vaultId, current);
    const owner = new Client({ connectionString: databaseUrl });
    await owner.connect();
    const key = `persistio:contradiction-activation:${vaultId}`;
    await owner.query('SELECT pg_advisory_lock(hashtextextended($1, 0))', [key]);
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    try {
      await drain(extractor as never);
      expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
      await owner.query('SELECT pg_advisory_unlock(hashtextextended($1, 0))', [key]);
      await dueNow(vaultId);
      await drain(extractor as never);
      expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
      expect((await owner.query('SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked', [key])).rows[0].locked).toBe(true);
    } finally { await owner.end(); }
  });

  it('refreshes generations for validity edits and removes expired, inactive, deleted work atomically', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const memoryId = await addMemory(vaultId, 'Dated fact.', { validFrom: '2099-01-01' });
    const initial = (await testPool.query("SELECT generation FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [memoryId])).rows[0].generation;
    await testPool.query('UPDATE memories SET valid_from = NULL WHERE id = $1', [memoryId]);
    const next = (await testPool.query("SELECT generation, available_at <= now() AS ready FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [memoryId])).rows[0];
    expect(next.generation).not.toBe(initial);
    expect(next.ready).toBe(true);
    expect((await testPool.query("DELETE FROM memory_contradiction_schedule WHERE memory_id = $1 AND generation = $2 AND policy = 'approved_only'", [memoryId, initial])).rowCount).toBe(0);
    await testPool.query("UPDATE memories SET valid_until = (now() AT TIME ZONE 'UTC')::date - 1 WHERE id = $1", [memoryId]);
    expect((await testPool.query('SELECT * FROM memory_contradiction_schedule WHERE memory_id = $1', [memoryId])).rowCount).toBe(0);
    const candidate = await addMemory(vaultId, 'Candidate.', { status: 'candidate', validFrom: '2099-01-01' });
    expect((await testPool.query('SELECT * FROM memory_contradiction_schedule WHERE memory_id = $1', [candidate])).rowCount).toBe(0);
    await testPool.query("UPDATE memories SET status = 'active', valid_from = NULL WHERE id = $1", [candidate]);
    expect((await testPool.query("SELECT * FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [candidate])).rowCount).toBe(1);
    await testPool.query('DELETE FROM memories WHERE id = $1', [candidate]);
    expect((await testPool.query("SELECT pending_count::int AS count FROM memory_contradiction_pending_vaults WHERE vault_id = $1 AND policy = 'approved_only'", [vaultId])).rows[0].count).toBe(0);
  });

  it('does not requeue no-op edits but tracks authority-invalidating subject changes', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const memoryId = await addMemory(vaultId, 'Stable fact.');
    await testPool.query('DELETE FROM memory_contradiction_schedule WHERE memory_id = $1', [memoryId]);
    await testPool.query('UPDATE memories SET confidence = confidence, valid_from = valid_from, updated_at = now() WHERE id = $1', [memoryId]);
    expect((await testPool.query('SELECT * FROM memory_contradiction_schedule WHERE memory_id = $1', [memoryId])).rowCount).toBe(0);
    await testPool.query("UPDATE memories SET subject = 'Changed subject' WHERE id = $1", [memoryId]);
    expect((await testPool.query("SELECT * FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [memoryId])).rowCount).toBe(1);
  });

  it('preserves a concurrent enqueue while advancing the pending-vault visit', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    await addMemory(vaultId, 'Only initial memory.');
    const originalQuery = Client.prototype.query;
    let added: string | undefined;
    const intercept = function (this: Client, ...args: unknown[]) {
      const result = Reflect.apply(originalQuery, this, args);
      if (!added && typeof args[0] === 'string' && args[0].includes('SELECT available_at FROM memory_contradiction_schedule')) {
        return Promise.resolve(result).then(async rows => {
          added = await addMemory(vaultId, 'Arrived after next-date snapshot.');
          return rows;
        });
      }
      return result;
    };
    const spy = vi.spyOn(Client.prototype, 'query').mockImplementation(intercept as typeof Client.prototype.query);
    try {
      await drain({ arbitrateConflict: vi.fn() } as never);
    } finally { spy.mockRestore(); }
    expect(added).toBeDefined();
    const state = (await testPool.query(
      `SELECT pending_count::int AS count, next_visit_at <= now() AS ready
       FROM memory_contradiction_pending_vaults WHERE vault_id = $1 AND policy = 'approved_only'`, [vaultId]
    )).rows[0];
    expect(state).toEqual({ count: 1, ready: true });
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
  });

  it('cannot mutate after losing its advisory-lock connection during the model call', async () => {
    await clearSchedules();
    const vaultId = await newVault();
    const old = await addMemory(vaultId, 'Old fact.');
    const current = await addMemory(vaultId, 'Current fact.');
    await onlySchedule(vaultId, current);
    const extractor = { arbitrateConflict: vi.fn(async () => {
      const terminated = await testPool.query(
        `SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity
         WHERE datname = current_database()
           AND application_name = 'persistio:contradiction-activation'`
      );
      expect(terminated.rows).toEqual([{ stopped: true }]);
      return 'supersede_old';
    }) };
    await expect(drain(extractor as never)).rejects.toThrow();
    expect((await testPool.query('SELECT status FROM memories WHERE id = ANY($1::uuid[])', [[old, current]])).rows.map(row => row.status))
      .toEqual(['active', 'active']);
    expect((await testPool.query('SELECT 1 FROM contradiction_scan_log WHERE vault_id = $1', [vaultId])).rowCount).toBe(0);
    expect((await testPool.query("SELECT 1 FROM memory_contradiction_schedule WHERE memory_id = $1 AND policy = 'approved_only'", [current])).rowCount).toBe(1);
    const retry = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(retry as never);
    expect(retry.arbitrateConflict).toHaveBeenCalledOnce();
  });

  it('backfills pre-upgrade active rows with durable priorities and supports transactional rollback', async () => {
    await clearSchedules();
    const client = await testPool.connect();
    const vaultId = crypto.randomUUID();
    await client.query('BEGIN');
    try {
      await client.query(fs.readFileSync(new URL('../../db/migrations/down/053_contradiction_activation_schedule.sql', import.meta.url), 'utf8'));
      await client.query('INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)',
        [vaultId, `activation-upgrade-${vaultId}`, crypto.randomUUID()]);
      const dates = (await client.query<{ yesterday: string; tomorrow: string }>(
        `SELECT ((now() AT TIME ZONE 'UTC')::date - 1)::text AS yesterday,
                ((now() AT TIME ZONE 'UTC')::date + 1)::text AS tomorrow`
      )).rows[0];
      const fixtures: Array<{ id: string; label: string; approved: boolean; from: string | null; until: string | null; legacy?: boolean }> = [
        { id: crypto.randomUUID(), label: 'current-approved', approved: true, from: null, until: null },
        { id: crypto.randomUUID(), label: 'future-approved', approved: true, from: dates.tomorrow, until: null },
        { id: crypto.randomUUID(), label: 'current-unapproved', approved: false, from: null, until: null },
        { id: crypto.randomUUID(), label: 'expired-approved', approved: true, from: null, until: dates.yesterday },
        { id: crypto.randomUUID(), label: 'legacy-authorized', approved: false, from: null, until: null, legacy: true }
      ];
      for (const fixture of fixtures) {
        await client.query(
          `INSERT INTO memories (id, vault_id, data, subject, hash, embedding, scope, scope_key,
             status, valid_from, valid_until, type)
           VALUES ($1, $2, $3, 'upgrade', $4, $5::vector, $6, $7, 'active', $8, $9, $10)`,
          [fixture.id, vaultId, fixture.label, crypto.randomUUID(), vector,
            fixture.legacy ? 'global' : 'project', fixture.legacy ? null : 'upgrade',
            fixture.from, fixture.until, fixture.legacy ? 'user_rule' : null]
        );
        if (fixture.approved) {
          await client.query("UPDATE memories SET authority_state = 'approved' WHERE id = $1", [fixture.id]);
          await client.query(
            `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
               new_version, actor_type, source, reason)
             SELECT $1, id, 'approve', 'approved', authority_version, 'user', 'api', 'upgrade fixture approval'
             FROM memories WHERE id = $2`, [vaultId, fixture.id]
          );
        }
        if (fixture.legacy) {
          await client.query(
            `INSERT INTO memory_authority_events (vault_id, memory_id, event_type, new_state,
               new_version, actor_type, source, reason, snapshot)
             SELECT $1, id, 'migration', 'proposed', authority_version, 'system', 'migration',
                    'upgrade legacy fixture', jsonb_build_object('type', type, 'scope', scope,
                      'status', status, 'archived_at', archived_at)
             FROM memories WHERE id = $2`, [vaultId, fixture.id]
          );
        }
      }
      await client.query(fs.readFileSync(new URL('../../db/migrations/053_contradiction_activation_schedule.sql', import.meta.url), 'utf8'));
      const schedules = await client.query(
        `SELECT m.data, s.policy, s.authority_ready, s.available_at <= now() AS due,
                (s.available_at AT TIME ZONE 'UTC')::date = m.valid_from AS starts_on_valid_from
         FROM memory_contradiction_schedule s JOIN memories m ON m.id = s.memory_id
         WHERE s.vault_id = $1 ORDER BY m.data, s.policy`, [vaultId]
      );
      expect(schedules.rows).toEqual(['current-approved', 'current-unapproved', 'future-approved', 'legacy-authorized'].flatMap(data =>
        ['approved_only', 'legacy', 'off'].map(policy => ({
          data, policy,
          authority_ready: data === 'legacy-authorized' ? policy === 'legacy' : data !== 'current-unapproved',
          due: data !== 'future-approved',
          starts_on_valid_from: data === 'future-approved' ? true : null
        }))));
      expect((await client.query(
        `SELECT policy, pending_count::int AS count, next_visit_at <= now() AS ready
         FROM memory_contradiction_pending_vaults WHERE vault_id = $1 ORDER BY policy`, [vaultId]
      )).rows).toEqual([
        { policy: 'approved_only', count: 2, ready: true },
        { policy: 'legacy', count: 3, ready: true },
        { policy: 'off', count: 2, ready: true }
      ]);
      expect((await client.query(
        'SELECT count(DISTINCT (policy, generation))::int AS generations FROM memory_contradiction_schedule WHERE vault_id = $1', [vaultId]
      )).rows[0].generations).toBe(12);
    } finally {
      await client.query('ROLLBACK');
      client.release();
    }
    expect((await testPool.query('SELECT 1 FROM vaults WHERE id = $1', [vaultId])).rowCount).toBe(0);
    expect((await testPool.query("SELECT to_regclass('memory_contradiction_schedule')::text AS relation")).rows[0].relation)
      .toBe('memory_contradiction_schedule');
  });

  const temporalCases = (['partial-overlap', 'one-unbounded', 'opposite-null-bounds'] as const).flatMap(windowCase =>
    ([false, true] as const).flatMap(reverse => ([false, true] as const).map(identical => ({ windowCase, reverse, identical }))));
  it.each(temporalCases)('preserves both horizons for $windowCase (reverse=$reverse, identical=$identical)', async ({ windowCase, reverse, identical }) => {
    await clearSchedules();
    const dates = (await testPool.query<{ before: string; today: string; after: string; later: string }>(
      `SELECT ((now() AT TIME ZONE 'UTC')::date - 5)::text AS before,
              ((now() AT TIME ZONE 'UTC')::date)::text AS today,
              ((now() AT TIME ZONE 'UTC')::date + 5)::text AS after,
              ((now() AT TIME ZONE 'UTC')::date + 10)::text AS later`
    )).rows[0];
    const windows: Array<{ validFrom: string | null; validUntil: string | null }> = windowCase === 'partial-overlap'
      ? [{ validFrom: dates.before, validUntil: dates.after }, { validFrom: dates.today, validUntil: dates.later }]
      : windowCase === 'one-unbounded'
        ? [{ validFrom: null, validUntil: null }, { validFrom: dates.today, validUntil: dates.later }]
        : [{ validFrom: null, validUntil: dates.later }, { validFrom: dates.before, validUntil: null }];
    if (reverse) windows.reverse();
    const vaultId = await newVault();
    await addMemory(vaultId, 'User lives in London.', windows[0]);
    const current = await addMemory(vaultId, identical ? 'User lives in London.' : 'User lives in Paris.', windows[1]);
    await onlySchedule(vaultId, current);
    const before = (await testPool.query(
      `SELECT id, data, confidence, valid_from::text, valid_until::text
       FROM memories WHERE vault_id = $1 ORDER BY id`, [vaultId]
    )).rows;
    const extractor = { arbitrateConflict: vi.fn(async () => 'supersede_old') };
    await drain(extractor as never);

    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    const after = (await testPool.query(
      `SELECT id, data, confidence, valid_from::text, valid_until::text, status
       FROM memories WHERE vault_id = $1 ORDER BY id`, [vaultId]
    )).rows;
    expect(after).toEqual(before.map(row => ({ ...row, status: 'needs_review' })));
    expect((await testPool.query('SELECT decision FROM contradiction_scan_log WHERE vault_id = $1', [vaultId])).rows)
      .toEqual([{ decision: 'needs_review' }]);
    expect((await testPool.query('SELECT 1 FROM memory_contradiction_schedule WHERE vault_id = $1', [vaultId])).rowCount).toBe(0);
  });

  const decisions = ['supersede_old', 'discard_new', 'merge', 'needs_review'] as const;
  const changes = ['content', 'validity', 'authority'] as const;
  const cases = decisions.flatMap(decision => (['current', 'candidate'] as const).flatMap(target =>
    changes.map(change => ({ decision, target, change }))));
  it.each(cases)('rejects $decision when $target $change changes during arbitration', async ({ decision, target, change }) => {
    await clearSchedules();
    const vaultId = await newVault();
    const candidate = await addMemory(vaultId, 'User lives in London.');
    const current = await addMemory(vaultId, 'User lives in Paris.');
    const changed = target === 'current' ? current : candidate;
    const extractor = { arbitrateConflict: vi.fn(async () => {
      if (change === 'content') await testPool.query("UPDATE memories SET data = 'Changed during model call' WHERE id = $1", [changed]);
      if (change === 'validity') await testPool.query("UPDATE memories SET valid_from = (now() AT TIME ZONE 'UTC')::date + 1 WHERE id = $1", [changed]);
      if (change === 'authority') await testPool.query("UPDATE memories SET authority_state = 'revoked', authority_version = authority_version + 1 WHERE id = $1", [changed]);
      return decision;
    }) };
    const client = new Client({ connectionString: databaseUrl });
    await client.connect();
    try {
      await expect(scan(vaultId, [current], extractor as never, { client, budget: { remaining: 1 } }))
        .rejects.toThrow('Contradiction inputs changed after arbitration');
      expect((await testPool.query('SELECT status FROM memories WHERE vault_id = $1', [vaultId])).rows.map(row => row.status))
        .toEqual(['active', 'active']);
      expect((await testPool.query('SELECT 1 FROM contradiction_scan_log WHERE vault_id = $1', [vaultId])).rowCount).toBe(0);
    } finally { await client.end(); }
  });
});
