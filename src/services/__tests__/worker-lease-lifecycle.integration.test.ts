import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('production worker lease lifecycle (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  let service: typeof import('../worker-lease');
  let db: typeof import('../../db/client');
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    db = await import('../../db/client');
    await db.runMigrations();
    service = await import('../worker-lease');
  });
  afterEach(async () => { await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]); vaults.length = 0; });
  afterAll(async () => { await pool.end(); await db?.closePool(); });

  async function fixture(kind: 'extraction' | 'curation' = 'extraction') {
    const vault = crypto.randomUUID(), segment = crypto.randomUUID(), queueId = crypto.randomUUID();
    const claimToken = crypto.randomUUID(), vaultClaimToken = crypto.randomUUID();
    vaults.push(vault);
    await pool.query('INSERT INTO vaults(id,name,api_key_hash) VALUES ($1,$2,$3)', [vault, 'lease-regression', crypto.randomUUID()]);
    await pool.query("INSERT INTO segments(id,vault_id,session_id,chunk_ids) VALUES ($1,$2,'lease','{}')", [segment, vault]);
    await pool.query(`INSERT INTO ${kind}_queue(id,vault_id,segment_id,claimed_at,claimed_by,claim_token,lease_expires_at${kind==='curation'?',work_key':''})
      VALUES ($1,$2,$3,now(),'worker',$4,now()+interval '10 minutes'${kind==='curation'?',gen_random_uuid()::text':''})`, [queueId, vault, segment, claimToken]);
    if (kind === 'curation') await pool.query(`INSERT INTO vault_curation_state(vault_id,curator_claimed_by,curator_claim_token,curator_claimed_until)
      VALUES ($1,'worker',$2,now()+interval '10 minutes')`, [vault, vaultClaimToken]);
    const lease: import('../worker-lease').WorkerLease = kind === 'curation'
      ? { queueKind: kind, queueId, claimToken, workerId: 'worker', vaultId: vault, vaultClaimToken }
      : { queueKind: kind, queueId, claimToken, workerId: 'worker' };
    return { lease, vault, table: `${kind}_queue`,
      row: async () => (await pool.query(`SELECT * FROM ${kind}_queue WHERE id=$1`, [queueId])).rows[0] };
  }

  it.each(['extraction', 'curation'] as const)('rejects expired %s release without takeover or any field change', async kind => {
    const f = await fixture(kind);
    await pool.query(`UPDATE ${f.table} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [f.lease.queueId]);
    const before = await f.row();
    expect(await service.releaseWorkerLease(f.lease, { incrementRetry: true, lastError: 'must not persist', availableAt: new Date(Date.now()+60_000) })).toBe(false);
    expect(await f.row()).toEqual(before);
  });

  it.each(['expiry', 'token', 'vault'] as const)('requires the actual curation vault capability (%s)', async invalid => {
    const f = await fixture('curation');
    if (invalid === 'expiry') await pool.query("UPDATE vault_curation_state SET curator_claimed_until=clock_timestamp()-interval '1 second' WHERE vault_id=$1", [f.vault]);
    const lease = { ...f.lease, ...(invalid === 'token' ? { vaultClaimToken: crypto.randomUUID() } : {}),
      ...(invalid === 'vault' ? { vaultId: crypto.randomUUID() } : {}) };
    const before = await f.row();
    expect(await service.releaseWorkerLease(lease, { incrementRetry: true })).toBe(false);
    expect(await f.row()).toEqual(before);
  });

  it.each(['release', 'renew', 'complete'] as const)('rejects %s that waited across expiry', async operation => {
    const f = await fixture();
    await pool.query(`UPDATE ${f.table} SET lease_expires_at=clock_timestamp()+interval '200 milliseconds' WHERE id=$1`, [f.lease.queueId]);
    const lock = await pool.connect();
    await lock.query('BEGIN');
    await lock.query(`SELECT id FROM ${f.table} WHERE id=$1 FOR UPDATE`, [f.lease.queueId]);
    const attempt = operation === 'release' ? service.releaseWorkerLease(f.lease)
      : operation === 'renew' ? service.renewWorkerLease(f.lease, 600_000)
      : db.withTransaction(async client => { await service.assertCurrentWorkerLease(client, f.lease); return true; })
        .catch(error => { if (error instanceof service.StaleWorkerLeaseError) return false; throw error; });
    try { await lock.query('SELECT pg_sleep(0.3)'); }
    finally { await lock.query('ROLLBACK'); lock.release(); }
    expect(await attempt).toBe(false);
    expect((await f.row()).claim_token).toBe(f.lease.claimToken);
  });

  for (const kind of ['extraction', 'curation'] as const) {
    for (const operation of ['renew', 'release', 'complete', 'dead-letter'] as const) {
      it.each(['expired', 'token', 'worker', 'missing'] as const)(`${kind} ${operation} rejects %s with no side effects`, async invalid => {
        const f = await fixture(kind);
        if (invalid === 'expired') await pool.query(`UPDATE ${f.table} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, [f.lease.queueId]);
        const before = await f.row();
        const lease = { ...f.lease,
          ...(invalid === 'token' ? { claimToken: crypto.randomUUID() } : {}),
          ...(invalid === 'worker' ? { workerId: 'other' } : {}),
          ...(invalid === 'missing' ? { queueId: crypto.randomUUID() } : {}) };
        if (operation === 'renew') expect(await service.renewWorkerLease(lease, 60_000)).toBe(false);
        else if (operation === 'release') expect(await service.releaseWorkerLease(lease, { incrementRetry: true })).toBe(false);
        else await expect(service.withWorkerLeaseTransaction(lease, async client => {
          await service.recordWorkerAction(client, lease, operation);
          await client.query(`DELETE FROM ${f.table} WHERE id=$1`, [lease.queueId]);
        })).rejects.toBeInstanceOf(service.StaleWorkerLeaseError);
        expect(await f.row()).toEqual(before);
        expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.lease.queueId])).rowCount).toBe(0);
      });
    }
    it(`${kind} final expiry rolls back receipts and mutations even after deleting the queue row`, async () => {
      const f = await fixture(kind);
      await pool.query(`UPDATE ${f.table} SET lease_expires_at=clock_timestamp()+interval '250 milliseconds' WHERE id=$1`, [f.lease.queueId]);
      await expect(service.withWorkerLeaseTransaction(f.lease, async client => {
        await service.recordWorkerAction(client, f.lease, 'complete');
        await client.query(`INSERT INTO memories(vault_id,data,subject,hash,scope) VALUES ($1,'no commit','test','test','global')`, [f.vault]);
        await client.query(`DELETE FROM ${f.table} WHERE id=$1`, [f.lease.queueId]);
        await client.query('SELECT pg_sleep(0.35)');
      })).rejects.toBeInstanceOf(service.StaleWorkerLeaseError);
      expect(await f.row()).toBeDefined();
      expect((await pool.query('SELECT 1 FROM memories WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
      expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.lease.queueId])).rowCount).toBe(0);
    });
    it(`${kind} live renewal and retry preserve identity then clear only the queue capability`, async () => {
      const f = await fixture(kind);
      expect(await service.renewWorkerLease(f.lease, 600_000)).toBe(true);
      const availableAt = new Date(Date.now()+60_000);
      expect(await service.releaseWorkerLease(f.lease, { incrementRetry: true, lastError: 'retry', availableAt })).toBe(true);
      expect(await f.row()).toMatchObject({ claim_token: null, claimed_by: null, lease_expires_at: null,
        retry_count: 1, last_error: 'retry', available_at: availableAt });
      if (kind === 'curation') expect((await pool.query('SELECT curator_claim_token FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].curator_claim_token)
        .toBe((f.lease as Extract<typeof f.lease, { queueKind: 'curation' }>).vaultClaimToken);
    });
  }

  it('checks expiry after waiting on the second curation lock', async () => {
    const f = await fixture('curation');
    await pool.query("UPDATE vault_curation_state SET curator_claimed_until=clock_timestamp()+interval '200 milliseconds' WHERE vault_id=$1", [f.vault]);
    const lock = await pool.connect();
    await lock.query('BEGIN');
    await lock.query('SELECT * FROM vault_curation_state WHERE vault_id=$1 FOR UPDATE', [f.vault]);
    const pending = service.renewWorkerLease(f.lease, 600_000);
    try { await lock.query('SELECT pg_sleep(0.3)'); }
    finally { await lock.query('ROLLBACK'); lock.release(); }
    expect(await pending).toBe(false);
  });

  it('rolls back queue renewal when the second update fails', async () => {
    const f = await fixture('curation');
    const original = await f.row();
    // A transaction-local trigger is installed only for this isolated fixture.
    await pool.query(`CREATE FUNCTION pr370_fail_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.vault_id='${f.vault}'::uuid THEN RAISE EXCEPTION 'second renewal failed'; END IF; RETURN NEW; END $$`);
    await pool.query('CREATE TRIGGER pr370_fail_renewal BEFORE UPDATE ON vault_curation_state FOR EACH ROW EXECUTE FUNCTION pr370_fail_renewal()');
    try { await expect(service.renewWorkerLease(f.lease, 60_000)).rejects.toThrow('second renewal failed'); }
    finally { await pool.query('DROP TRIGGER pr370_fail_renewal ON vault_curation_state'); await pool.query('DROP FUNCTION pr370_fail_renewal()'); }
    expect(await f.row()).toEqual(original);
  });

  it.each([null, {}, { queueKind: 'other' }, { queueKind: 'curation', queueId: crypto.randomUUID(), claimToken: crypto.randomUUID(), workerId: 'worker' }])('rejects malformed capabilities', async lease => {
    expect(await service.releaseWorkerLease(lease as never)).toBe(false);
  });
});
