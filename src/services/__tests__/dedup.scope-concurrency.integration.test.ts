import crypto from 'node:crypto';
import { Pool, type PoolClient } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { DedupInput, DedupOptions } from '../dedup';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('baseline deduplication invariants and concurrency (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  let db: typeof import('../../db/client');
  let dedup: typeof import('../dedup');
  let prepareVaultCrypto: typeof import('../crypto')['prepareVaultCrypto'];
  let vaultId: string, sourceA: string, sourceB: string;
  let vector: number[];
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    db = await import('../../db/client');
    await db.runMigrations();
    dedup = await import('../dedup');
    ({ prepareVaultCrypto } = await import('../crypto'));
    (await import('../../config')).getConfig().ENCRYPTION_ENABLED = false;
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector = [1, ...Array(Number(/\((\d+)\)/.exec(type)![1]) - 1).fill(0)];
  });
  beforeEach(async () => {
    vaultId = crypto.randomUUID(); sourceA = crypto.randomUUID(); sourceB = crypto.randomUUID();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'dedup-restoration',$2,'unlimited')", [vaultId, crypto.randomUUID()]);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role) SELECT id,$1,'s1','user' FROM unnest($2::uuid[]) AS id", [vaultId, [sourceA,sourceB]]);
  });
  afterEach(async () => { await pool.query('DELETE FROM vaults WHERE id=$1', [vaultId]); });
  afterAll(async () => { await pool.end(); await db?.closePool(); });

  function input(extra: Partial<DedupInput> = {}): DedupInput {
    return { vaultId, fact:'The service uses PostgreSQL.', subject:'service', embedding:vector, sourceChunks:[sourceA],
      score:8, salience:0.8, sensitivity:'low', type:'system_fact', scope:'session', scopeKey:'s1',
      polarity:'neutral', status:'active', volatility:'low', validFrom:null, validUntil:null,
      sourceTimestamp:'2025-01-01T00:00:00Z', ...extra };
  }
  const read = async (id: string) => (await pool.query('SELECT *,revision::text AS revision FROM memories WHERE id=$1', [id])).rows[0];
  async function preparedDecision(value: DedupInput, decision: DedupOptions['precomputedConflictDecision'] = 'supersede_old') {
    const request = await dedup.getDedupEscalationRequest(value, 'test');
    expect(request).not.toBeNull();
    return { precomputedConflictDecision:decision, precomputedConflictInput:request!.inputFingerprint,
      precomputedConflictMemoryId:request!.existingMemoryId, precomputedConflictMemoryRevision:request!.existingMemoryRevision };
  }

  it('holds startup behind the shared migration advisory lock', async () => {
    const blocker = await pool.connect();
    await blocker.query('SELECT pg_advisory_lock($1::bigint)', [db.MIGRATION_ADVISORY_LOCK_ID]);
    let completed = false;
    const migration = db.runMigrations().then(() => { completed = true; });
    try {
      let waiting = false;
      for (let i=0;i<100 && !waiting;i++) {
        waiting = Number((await pool.query("SELECT count(*) AS count FROM pg_locks WHERE locktype='advisory' AND NOT granted")).rows[0].count)>0;
        if (!waiting) await new Promise(resolve => setTimeout(resolve, 10));
      }
      expect(waiting).toBe(true); expect(completed).toBe(false);
    } finally {
      await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [db.MIGRATION_ADVISORY_LOCK_ID]); blocker.release();
    }
    await migration; expect(completed).toBe(true);
  });
  it('rejects missing/invalid bindings at both application and database boundaries', async () => {
    for (const value of [{scope:undefined}, {scope:'vault'}, {scope:'session',scopeKey:null}, {scope:'global',scopeKey:'s1'}]) {
      await expect(dedup.deduplicateMemory(input(value as any))).rejects.toThrow('binding');
    }
    await expect(pool.query("INSERT INTO memories(/* EXPECT_SCOPE_CONSTRAINT_FAILURE */ vault_id,data,subject,hash) VALUES($1,'Fact','subject',$2)",[vaultId,crypto.randomUUID()])).rejects.toMatchObject({code:'23502'});
    await expect(pool.query("INSERT INTO memories(vault_id,data,subject,hash,scope) VALUES($1,'Fact','subject',$2,'vault')",[vaultId,crypto.randomUUID()])).rejects.toMatchObject({code:'23514'});
  });
  it('serializes concurrent exact retries without duplicate memories, quota or premium work', async () => {
    const results = await Promise.all(Array.from({length:4}, () => dedup.deduplicateMemory(input())));
    expect(results.filter(r=>r.action==='inserted')).toHaveLength(1);
    expect(new Set(results.map(r=>r.memoryId)).size).toBe(1);
    const row = await read(results[0].memoryId!);
    expect(row.revision).toBe('1'); expect(row.source_chunks).toEqual([sourceA]);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[vaultId])).rowCount).toBe(1);
    expect((await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[vaultId])).rowCount).toBe(1);
  });
  it('unions new evidence, preserves strongest sensitivity and latest source time, and leaves exact retries unchanged', async () => {
    const first = await dedup.deduplicateMemory(input());
    await dedup.deduplicateMemory(input({sensitivity:'high'}));
    expect(await read(first.memoryId!)).toMatchObject({revision:'2',sensitivity:'high',status:'active'});
    await dedup.deduplicateMemory(input({sourceChunks:[sourceB],sourceTimestamp:'2025-02-01T00:00:00Z'}));
    const updated = await read(first.memoryId!);
    expect(updated.source_chunks.sort()).toEqual([sourceA,sourceB].sort());
    expect(updated.sensitivity).toBe('high');
    expect(updated.source_timestamp.toISOString()).toBe('2025-02-01T00:00:00.000Z');
    await dedup.deduplicateMemory(input());
    expect((await read(first.memoryId!)).revision).toBe(updated.revision);
  });
  it('keeps identical facts distinct across every scope binding and dated state', async () => {
    const variants: Partial<DedupInput>[] = [
      {}, {scopeKey:'s2'}, {scope:'task',scopeKey:'t1'}, {scope:'project',scopeKey:'p1'}, {scope:'global',scopeKey:null},
      {validFrom:'2020-01-01',validUntil:'2020-12-31'}, {validFrom:'2021-01-01',validUntil:'2021-12-31'},
      {validFrom:'2020-06-01',validUntil:'2022-01-01'}, {type:'decision'}, {polarity:'negative'}, {subject:'different-entity'}
    ];
    for (const variant of variants) expect((await dedup.deduplicateMemory(input(variant))).action).toBe('inserted');
    const rows=(await pool.query('SELECT status,archived_at FROM memories WHERE vault_id=$1',[vaultId])).rows;
    expect(rows).toHaveLength(variants.length);
    expect(rows.every(r=>r.status==='active' && r.archived_at===null)).toBe(true);
  });
  it.each(['superseded','contradicted','archived'])('never consolidates into a %s row', async state => {
    const first=await dedup.deduplicateMemory(input());
    await pool.query("UPDATE memories SET status=$2,archived_at=CASE WHEN $3 THEN now() ELSE NULL END WHERE id=$1",
      [first.memoryId,state==='archived'?'active':state,state==='archived']);
    const next=await dedup.deduplicateMemory(input());
    expect(next.action).toBe('inserted'); expect(next.memoryId).not.toBe(first.memoryId);
  });
  it.each(['scope','dates','content','archive','delete','incoming'])('does not apply a prepared retirement after %s changes', async change => {
    const first=await dedup.deduplicateMemory(input());
    let incoming=input({fact:'The service now uses SQLite.'});
    const options=await preparedDecision(incoming);
    if(change==='scope') await pool.query("UPDATE memories SET scope_key='s2' WHERE id=$1",[first.memoryId]);
    if(change==='dates') await pool.query("UPDATE memories SET valid_until='2020-01-01' WHERE id=$1",[first.memoryId]);
    if(change==='content') await pool.query("UPDATE memories SET data='Independently corrected fact' WHERE id=$1",[first.memoryId]);
    if(change==='archive') await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[first.memoryId]);
    if(change==='delete') await pool.query('DELETE FROM memories WHERE id=$1',[first.memoryId]);
    if(change==='incoming') incoming={...incoming,fact:'A different new fact.'};
    const result=await dedup.deduplicateMemory(incoming,undefined,options);
    expect(result.action).toBe('inserted');
    if(change!=='delete') expect((await read(first.memoryId!)).status).toBe('active');
  });
  it('retains both alternatives and explicit revision-bound uncertainty without an authoritative decision', async () => {
    const first=await dedup.deduplicateMemory(input());
    const next=await dedup.deduplicateMemory(input({fact:'The service uses SQLite.'}));
    expect((await read(first.memoryId!)).status).toBe('active');
    expect((await read(next.memoryId!)).status).toBe('active');
    expect((await pool.query('SELECT decision,revision_a::text,revision_b::text FROM contradiction_scan_log WHERE vault_id=$1',[vaultId])).rows)
      .toEqual([{decision:'keep_both',revision_a:'1',revision_b:'1'}]);
  });
  it('preserves old text and source lineage for a revision-bound equivalent observation', async () => {
    const first=await dedup.deduplicateMemory(input());
    const incoming=input({fact:'PostgreSQL is used by the service.',sourceChunks:[sourceB],sensitivity:'high',sourceTimestamp:'2025-03-01T00:00:00Z'});
    const result=await dedup.deduplicateMemory(incoming,undefined,await preparedDecision(incoming,'merge'));
    expect(result).toEqual({action:'updated',memoryId:first.memoryId});
    expect(await read(first.memoryId!)).toMatchObject({data:input().fact,sensitivity:'high',revision:'2'});
    expect((await read(first.memoryId!)).source_chunks.sort()).toEqual([sourceA,sourceB].sort());
  });
  it('rolls back retirement, quota and audit when replacement insertion fails after retirement', async () => {
    const first=await dedup.deduplicateMemory(input());
    const incoming=input({fact:'The service uses SQLite.'});
    const options=await preparedDecision(incoming);
    const vault=(await pool.query('SELECT * FROM vaults WHERE id=$1',[vaultId])).rows[0];
    const prepared=await prepareVaultCrypto(vault);
    let retired=false;
    await expect(db.withTransaction(async client=>{
      const intercepted={query:async(sql:string,values?:unknown[])=>{
        if(sql.includes('INSERT INTO memories')) throw new Error('injected late insert failure');
        const result=await client.query(sql,values);
        if(sql.includes("SET status='superseded'")) retired=true;
        return result;
      }} as PoolClient;
      return dedup.deduplicateMemoryInTransaction(incoming,intercepted,prepared,[],options);
    })).rejects.toThrow('injected late insert failure');
    expect(retired).toBe(true);
    expect(await read(first.memoryId!)).toMatchObject({status:'active',archived_at:null,revision:'1'});
    expect((await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[vaultId])).rowCount).toBe(1);
    expect((await pool.query('SELECT id FROM memories WHERE vault_id=$1',[vaultId])).rowCount).toBe(1);
  });
  it('counts retained history toward capacity and serializes concurrent creation at the limit', async () => {
    await pool.query("UPDATE vaults SET rate_limit_override='{\"memories_max\":2}'::jsonb WHERE id=$1",[vaultId]);
    const historical=await dedup.deduplicateMemory(input({validUntil:'2020-01-01'}));
    const attempts=await Promise.allSettled([
      dedup.deduplicateMemory(input({subject:'topic-a',fact:'A second durable fact.'})),
      dedup.deduplicateMemory(input({subject:'topic-b',fact:'A third durable fact.'}))
    ]);
    expect(attempts.filter(r=>r.status==='fulfilled')).toHaveLength(1);
    expect(attempts.filter(r=>r.status==='rejected')).toHaveLength(1);
    await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[historical.memoryId]);
    expect((await dedup.deduplicateMemory(input({subject:'topic-c',fact:'A replacement for archived storage.'}))).action).toBe('inserted');
  });
  it('refuses foreign or missing source IDs without partial memory or work', async () => {
    await expect(dedup.deduplicateMemory(input({sourceChunks:[crypto.randomUUID()]}))).rejects.toThrow('does not belong');
    expect((await pool.query('SELECT id FROM memories WHERE vault_id=$1',[vaultId])).rowCount).toBe(0);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[vaultId])).rowCount).toBe(0);
  });
});
