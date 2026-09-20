import {fixtureSource,cleanFixtureSources,fixtureExtractor,fixtureAssessment} from '../../__tests__/arbitration-fixtures';
import crypto from 'node:crypto';
import { Client, Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DedupInput } from '../dedup';

// Only KMS unwrap and the arbitration provider are mocked. Both memories enter
// through production dedup (including quota, evidence, encryption and triggers),
// then the production activation drain owns discovery, locks and pair completion.
const kms = vi.hoisted(() => ({unwrap:vi.fn()}));
vi.mock('@google-cloud/kms', () => ({KeyManagementServiceClient:class {decrypt = kms.unwrap;}}));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const behavioral = ['user_rule','user_preference','task_pattern','workflow','constraint','decision'] as const;

describe.skipIf(!databaseUrl)('dedup to future contradiction activation (PostgreSQL 17)', () => {
  const pool = new Pool({connectionString:databaseUrl,options:'-c timezone=Pacific/Honolulu -c statement_timeout=10000'});
  const vaultIds:string[] = [], dek = Buffer.alloc(32,47);
  let db:typeof import('../../db/client'), dedup:typeof import('../dedup'), crypt:typeof import('../crypto');
  let drain:typeof import('../contradiction-activation').drainDueContradictionActivations;
  let config:ReturnType<typeof import('../../config').getConfig>;
  let vector:number[], activationDay:string, beforeActivation:Date;
  beforeAll(async () => {
    if(!/(?:^|_)(?:test|temporal)(?:_|$)/.test(new URL(databaseUrl!).pathname.slice(1))) {
      throw new Error('Combined dedup activation integration requires an isolated test/temporal database');
    }
    if(process.env.DATABASE_URL !== databaseUrl) throw new Error('Set DATABASE_URL and PERSISTIO_TEST_DATABASE_URL to the same isolated test database');
    // Give the real application pool AND every fresh scheduler connection a
    // non-UTC timezone; preserve the caller's URL outside this isolated worker.
    const url = new URL(databaseUrl!);
    url.searchParams.set('options','-c timezone=Pacific/Honolulu -c statement_timeout=10000');
    process.env.DATABASE_URL = url.toString();
    db = await import('../../db/client'); await db.runMigrations();
    config = (await import('../../config')).getConfig();
    config.ENCRYPTION_ENABLED = true; config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    crypt = await import('../crypto'); await crypt.initCryptoClient();
    dedup = await import('../dedup');
    ({drainDueContradictionActivations:drain} = await import('../contradiction-activation'));
    const server = (await db.query("SELECT current_setting('server_version_num')::int AS version,current_setting('TimeZone') AS timezone")).rows[0];
    expect(server.version).toBeGreaterThanOrEqual(170000); expect(server.version).toBeLessThan(180000);
    expect(server.timezone).toBe('Pacific/Honolulu');
    activationDay = (await pool.query("SELECT (statement_timestamp() AT TIME ZONE 'UTC')::date::text AS day")).rows[0].day;
    beforeActivation = new Date(Date.parse(activationDay+'T00:00:00Z')-1);
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector = [1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)];
    expect((await pool.query("SELECT tgenabled FROM pg_trigger WHERE tgrelid='memories'::regclass AND tgname='memories_maintain_revision'")).rows)
      .toEqual([{tgenabled:'O'}]);
  });
  beforeEach(() => {
    kms.unwrap.mockResolvedValue([{plaintext:dek}]);
    config.CONTRADICTION_SCAN_ENABLED = true;
    config.CONTRADICTION_SCAN_MIN_SIMILARITY = 0.8;
    config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH = 1;
    vi.useFakeTimers({toFake:['Date']}); vi.setSystemTime(beforeActivation);
  });
  afterEach(async () => {
    await cleanFixtureSources();
    vi.useRealTimers(); vi.restoreAllMocks(); kms.unwrap.mockReset();
    if(vaultIds.length) await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaultIds]);
    vaultIds.length = 0;
  });
  afterAll(async () => {await pool.end(); await db?.closePool();});

  const states = async (v:string) => (await pool.query(`SELECT id,data,subject,subject_encrypted,subject_hmac,hash,
    type,polarity,valid_from::text,valid_until::text,status,revision::text,source_chunks,evidence
    FROM memories WHERE vault_id=$1 ORDER BY id`,[v])).rows;
  const logs = async (v:string) => (await pool.query(`SELECT memory_id_a,memory_id_b,decision,
    revision_a::text,revision_b::text FROM contradiction_scan_log WHERE vault_id=$1`,[v])).rows;
  const schedules = async (v:string) => (await pool.query(`SELECT memory_id,revision::text,generation,available_at,
    failures,scan_memory_id,scan_reference_date,scan_similarity FROM memory_contradiction_schedule WHERE vault_id=$1 ORDER BY memory_id`,[v])).rows;
  const adds = async (v:string) => Number((await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1',[v])).rows[0].memory_adds);
  async function wake(v:string) {
    await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 minute' WHERE vault_id=$1",[v]);
  }
  function clock(now:Date) {
    vi.setSystemTime(now);
    const original = Client.prototype.query, connections = new Set<Client>();
    const hook = vi.spyOn(Client.prototype,'query').mockImplementation(function(this:Client,...args:unknown[]) {
      if(typeof args[0] === 'string') {
        if(args[0].includes('SELECT memory_id, generation, failures')) connections.add(this);
        // Execute real SQL with a controlled reference instant and matching
        // persistence fence. Never fabricate rows or change memory revisions.
        // Queue due times use real now(); activationDay is the real UTC day.
        args[0] = args[0].replaceAll('statement_timestamp()',`TIMESTAMPTZ '${now.toISOString()}'`);
      }
      return Reflect.apply(original,this,args);
    } as any);
    return {hook,connections};
  }

  it.each([false,true].flatMap(encrypted => behavioral.map(type => ({encrypted,type}))))
  ('retains single-entry $type future preflight across the write boundary (encrypted=$encrypted)', async ({encrypted,type}) => {
    const v = crypto.randomUUID(); vaultIds.push(v);
    await pool.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id,encrypted_dek,vault_encryption_enabled)
      VALUES($1,'midnight-dedup',$2,'unlimited',$3,$4)`,[v,crypto.randomUUID(),encrypted?'synthetic-midnight-key':null,encrypted]);
    const input:DedupInput = {vaultId:v,fact:'Use the first workflow.',subject:'topic',embedding:vector,
      sourceChunks:[await fixtureSource(pool,v)],score:8,salience:0.8,sensitivity:'low',type,scope:'session',scopeKey:'s1',
      polarity:'neutral',status:'active',volatility:'low',validFrom:activationDay,validUntil:activationDay};
    const extractor = fixtureExtractor(async () => 'keep_both');
    await dedup.deduplicateMemory(input,extractor as never);
    const transaction = db.withTransaction;
    const boundary = vi.spyOn(db,'withTransaction').mockImplementationOnce(async fn => {
      vi.setSystemTime(new Date(activationDay+'T00:00:00Z'));
      return transaction(fn);
    });
    await dedup.deduplicateMemory({...input,fact:'Use the second workflow.'},extractor as never);
    expect(boundary).toHaveBeenCalledOnce(); boundary.mockRestore();
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await logs(v)).toEqual([]);
    expect(await states(v)).toHaveLength(2); expect(await adds(v)).toBe(2);
    expect(await schedules(v)).toHaveLength(2);
    const active = clock(new Date(activationDay+'T00:00:00Z'));
    await wake(v); await drain(extractor as never);
    await wake(v); await drain(extractor as never);
    active.hook.mockRestore();
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(await logs(v)).toHaveLength(1); expect(await schedules(v)).toEqual([]);
  });

  it.each([false,true].flatMap(encrypted => behavioral.map(type => ({encrypted,type}))))
  ('defers $type dedup arbitration until activation exactly once (encrypted=$encrypted)', async ({encrypted,type}) => {
    const v = crypto.randomUUID(); vaultIds.push(v);
    const vault = {id:v,encrypted_dek:encrypted?'synthetic-dedup-activation-key':null,vault_encryption_enabled:encrypted};
    await pool.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id,encrypted_dek,vault_encryption_enabled)
      VALUES($1,'dedup-activation',$2,'unlimited',$3,$4)`,[v,crypto.randomUUID(),vault.encrypted_dek,encrypted]);
    const sources = [crypto.randomUUID(),crypto.randomUUID()];
    for(const source of sources)await fixtureSource(pool,v,undefined,source);
    const input:DedupInput = {vaultId:v,fact:'Use the first workflow.',subject:'topic',embedding:vector,
      sourceChunks:[sources[0]],score:8,salience:0.8,sensitivity:'low',type,scope:'session',scopeKey:'s1',
      polarity:'neutral',status:'active',volatility:'low',validFrom:activationDay,validUntil:activationDay,
      evidence:JSON.stringify({quote:'Use the first workflow.'})};
    const incoming:DedupInput = {...input,fact:'Use the second workflow.',sourceChunks:[sources[1]],
      evidence:JSON.stringify({quote:'Use the second workflow.'})};
    const extractor = fixtureExtractor(async () => 'keep_both');
    // Exercise the public entry point, not manual inserts or a mocked match.
    const first = await dedup.deduplicateMemory(input,extractor as never);
    const second = await dedup.deduplicateMemory(incoming,extractor as never);
    expect(first.action).toBe('inserted'); expect(second.action).toBe('inserted');
    expect(second.memoryId).not.toBe(first.memoryId);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await logs(v)).toEqual([]); // The old unconditional log fails here.
    expect(await adds(v)).toBe(2);
    const before = await states(v), initialSchedules = await schedules(v);
    expect(before).toHaveLength(2); expect(initialSchedules).toHaveLength(2);
    const prepared = await crypt.prepareVaultCrypto(vault);
    for(const [result,value] of [[first,input],[second,incoming]] as const) {
      const stored = before.find(r => r.id === result.memoryId)!;
      expect(stored).toMatchObject({type,polarity:'neutral',valid_from:activationDay,valid_until:activationDay,
        status:'active',revision:'1',source_chunks:value.sourceChunks,evidence:{quote:value.fact}});
      expect(prepared.decrypt(vault,stored.data)).toBe(value.fact);
      if(encrypted) {
        expect(stored.data).not.toBe(value.fact); expect(stored.subject).toBe('');
        expect(stored.subject_encrypted).toBeTruthy(); expect(stored.subject_hmac).toBeTruthy();
      }
    }
    expect((await pool.query('SELECT count(*)::int AS n FROM memory_embeddings WHERE memory_id=ANY($1::uuid[])',
      [[first.memoryId,second.memoryId]])).rows[0].n).toBe(2);
    expect(await dedup.deduplicateMemory(incoming,extractor as never)).toEqual({action:'skipped',memoryId:second.memoryId});
    expect(await adds(v)).toBe(2); expect(await states(v)).toEqual(before);
    expect(await schedules(v)).toEqual(initialSchedules);

    // An immediate scanner visit before applicability must retain the reminder
    // even though same-metadata distinct hashes are excluded by its SQL shortlist.
    const early = clock(beforeActivation);
    await wake(v); await drain(extractor as never); early.hook.mockRestore();
    expect(early.connections.size).toBe(1);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled(); expect(await logs(v)).toEqual([]);
    expect(await states(v)).toEqual(before);
    const retained = await schedules(v);
    expect(retained).toHaveLength(2);
    for(const reminder of retained) {
      expect(reminder).toMatchObject({revision:'1',failures:0,scan_memory_id:null,scan_reference_date:null,scan_similarity:null});
      expect(reminder.generation).toBe(initialSchedules.find(r => r.memory_id === reminder.memory_id)!.generation);
    }
    expect(retained.some(r => r.available_at.getTime() === Date.parse(activationDay+'T00:00:00Z'))).toBe(true);

    // No memory mutation or manual reminder insertion to make the pair eligible.
    const active = clock(new Date(activationDay+'T00:00:00Z'));
    await wake(v); await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    const [existingFact,newFact,vaultId,context] = extractor.arbitrateConflict.mock.calls[0] as unknown as
      [string,string,string,{existing:{validFrom:string;validUntil:string};incoming:{validFrom:string;validUntil:string}}];
    expect([existingFact,newFact].sort()).toEqual([input.fact,incoming.fact].sort()); expect(vaultId).toBe(v);
    expect(context).toMatchObject({existing:{validFrom:activationDay,validUntil:activationDay},
      incoming:{validFrom:activationDay,validUntil:activationDay}});
    const completed = await logs(v);
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({decision:'keep_both',revision_a:'1',revision_b:'1'});
    expect([completed[0].memory_id_a,completed[0].memory_id_b].sort()).toEqual([first.memoryId,second.memoryId].sort());
    expect(await states(v)).toEqual(before);
    // Drain the other original reminder, then an empty repeat. Pair orientation
    // must not matter, and keep_both must not create a substantive revision.
    await wake(v); await drain(extractor as never);
    await wake(v); await drain(extractor as never); active.hook.mockRestore();
    expect(active.connections.size).toBe(2); // Third drain has no pending vault to visit.
    expect([...active.connections].every(c => !early.connections.has(c))).toBe(true);
    expect(await schedules(v)).toEqual([]);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce(); expect(await logs(v)).toEqual(completed);
    expect(await states(v)).toEqual(before); expect(await adds(v)).toBe(2);
  });
});
