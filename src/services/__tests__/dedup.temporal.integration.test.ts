import {fixtureSource,cleanFixtureSources,fixtureAssessment} from '../../__tests__/arbitration-fixtures';
import crypto from 'node:crypto';
import { Pool, type PoolClient, type QueryResult, type QueryResultRow } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DedupInput, DedupOptions } from '../dedup';
import type { PreparedVaultCrypto } from '../crypto';

// No live provider calls. Keep actual AES-GCM, SQL, pgvector, migrations,
// revision/schedule triggers and transactional quota enforcement.
const kms = vi.hoisted(() => ({ unwrap:vi.fn() }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient:class { decrypt = kms.unwrap; } }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const NOW = new Date('2026-09-18T00:30:00Z');
const behavioral = ['user_rule','user_preference','task_pattern','workflow','constraint','decision'] as const;
const factual = ['project','system_fact','domain_knowledge'] as const;
const windows = [{validFrom:null,validUntil:'2026-09-17'},{validFrom:'2026-09-19',validUntil:null}];

describe.skipIf(!databaseUrl)('dedup temporal arbitration selection (PostgreSQL 17)', () => {
  const pool = new Pool({connectionString:databaseUrl,options:'-c timezone=America/Los_Angeles -c statement_timeout=10000'});
  const vaultIds:string[] = [], dek = Buffer.alloc(32,39);
  let db:typeof import('../../db/client'), dedup:typeof import('../dedup'), crypt:typeof import('../crypto');
  let config:ReturnType<typeof import('../../config').getConfig>;
  let vector:number[];
  beforeAll(async () => {
    // This suite mutates fixture vaults and runs migrations. Require an explicitly
    // selected test DB before connecting, and set BOTH URLs before any imports.
    if(!/(?:^|_)(?:test|temporal)(?:_|$)/.test(new URL(databaseUrl!).pathname.slice(1))) {
      throw new Error('Dedup temporal integration requires a test/temporal database name');
    }
    if(process.env.DATABASE_URL !== databaseUrl) throw new Error('Set DATABASE_URL and PERSISTIO_TEST_DATABASE_URL to the same isolated test database');
    const server = await pool.query("SELECT current_setting('server_version_num')::int AS version,current_setting('TimeZone') AS timezone");
    expect(server.rows[0].version).toBeGreaterThanOrEqual(170000);
    expect(server.rows[0].version).toBeLessThan(180000);
    expect(server.rows[0].timezone).toBe('America/Los_Angeles');
    db = await import('../../db/client'); await db.runMigrations();
    config = (await import('../../config')).getConfig();
    config.ENCRYPTION_ENABLED = true; config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    crypt = await import('../crypto'); await crypt.initCryptoClient();
    dedup = await import('../dedup');
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector = [1,...Array(Number(/\((\d+)\)/.exec(type)![1])-1).fill(0)];
    expect((await pool.query("SELECT tgenabled FROM pg_trigger WHERE tgrelid='memories'::regclass AND tgname='memories_maintain_revision'")).rows).toEqual([{tgenabled:'O'}]);
  });
  beforeEach(() => {
    kms.unwrap.mockResolvedValue([{plaintext:dek}]);
    vi.useFakeTimers({toFake:['Date']}); vi.setSystemTime(NOW);
  });
  afterEach(async () => {
    vi.useRealTimers(); vi.restoreAllMocks(); kms.unwrap.mockReset();
    await cleanFixtureSources();
    if(vaultIds.length) await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaultIds]);
    vaultIds.length = 0;
  });
  afterAll(async () => { await pool.end(); await db?.closePool(); });

  async function fixture(encrypted = false) {
    const id = crypto.randomUUID(); vaultIds.push(id);
    const vault = {id,encrypted_dek:encrypted?'synthetic-wrapped-key':null,vault_encryption_enabled:encrypted};
    await pool.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id,encrypted_dek,vault_encryption_enabled)
      VALUES($1,'dedup-temporal-selection',$2,'unlimited',$3,$4)`,[id,crypto.randomUUID(),vault.encrypted_dek,encrypted]);
    const prepared = await crypt.prepareVaultCrypto(vault);
    const input:DedupInput = {vaultId:id,fact:'Use the incoming workflow.',subject:'topic',embedding:vector,sourceChunks:[await fixtureSource(pool,id)],
      score:8,salience:0.8,sensitivity:'low',type:'workflow',scope:'session',scopeKey:'s1',polarity:'neutral',
      status:'active',volatility:'low',validFrom:null,validUntil:null};
    return {vault,prepared,input};
  }
  type Fixture = Awaited<ReturnType<typeof fixture>>;
  async function seed(f:Fixture, extra:Partial<DedupInput> = {}, similarity = 1, options:{id?:string;hash?:string;poison?:boolean} = {}) {
    const v = {...f.input,fact:'Stored workflow '+crypto.randomUUID(),...extra}, id = options.id ?? crypto.randomUUID();
    const embedding = [similarity,Math.sqrt(1-similarity**2),...vector.slice(2)];
    const subject = f.prepared.subject(f.vault,v.subject);
    await pool.query(`INSERT INTO memories(id,vault_id,data,subject,subject_encrypted,subject_hmac,hash,embedding,
      scope,scope_key,status,type,polarity,valid_from,valid_until,confidence,score,sensitivity,source_chunks)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8::vector,$9,$10,'active',$11,$12,$13::date,$14::date,0.9,8,$15,$16::uuid[])`,
    [id,f.vault.id,options.poison?'invalid-ciphertext':f.prepared.encrypt(f.vault,v.fact),
      f.vault.vault_encryption_enabled?'':v.subject,subject?.encrypted??null,subject?.hmac??null,
      options.hash??crypto.createHash('sha256').update(v.fact).digest('hex'),JSON.stringify(embedding),
      v.scope,v.scopeKey,v.type,v.polarity,v.validFrom,v.validUntil,v.sensitivity,v.sourceChunks]);
    await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)',[id,JSON.stringify(embedding)]);
    return id;
  }
  const state = async (f:Fixture) => (await pool.query(`SELECT id,data,type,status,archived_at,revision::text,source_chunks,
    valid_from::text,valid_until::text FROM memories WHERE vault_id=$1 ORDER BY id`,[f.vault.id])).rows;
  const decision = async (request:NonNullable<Awaited<ReturnType<typeof dedup.getDedupEscalationRequest>>>, action:DedupOptions['precomputedConflictDecision'] = 'merge'):Promise<DedupOptions> => ({
    precomputedConflictDecision:await fixtureAssessment(request,vaultIds[0],action as any),precomputedConflictInput:request.inputFingerprint,
    precomputedConflictMemoryId:request.existingMemoryId,precomputedConflictMemoryRevision:request.existingMemoryRevision
  });
  async function apply(f:Fixture, options:DedupOptions = {}, prepared:PreparedVaultCrypto = f.prepared) {
    // Use the same non-UTC pool for every dedup query; no worker is started and no
    // post-commit fire-and-forget effects race fixture cleanup.
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await dedup.deduplicateMemoryInTransaction(f.input,client,prepared,[],options);
      await client.query('COMMIT'); return result;
    } catch(error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  }
  async function preflight(f:Fixture) {
    const reads:Array<{sql:string;ids:string[];values?:unknown[]}> = [];
    const reader = {query:async <T extends QueryResultRow = QueryResultRow>(sql:string,values?:unknown[]):Promise<QueryResult<T>> => {
      const result = await pool.query<T>(sql,values);
      if(sql.includes('FROM memories m WHERE')) reads.push({sql,ids:result.rows.map(r => r.id),values});
      return result;
    }};
    const request = await dedup.getDedupEscalationRequest(f.input,'test',reader,f.prepared,NOW);
    return {request,reads};
  }

  it.each([false,true].flatMap(encrypted => behavioral.flatMap(type => windows.map(dates => ({encrypted,type,dates})))))
  ('selects current conflict below 12 higher-ranked non-current rows: %j', async ({encrypted,type,dates}) => {
    const f = await fixture(encrypted); f.input.type = type;
    const excluded:string[] = [];
    for(let i=0;i<12;i++) excluded.push(await seed(f,{type,...dates},0.99,{poison:encrypted}));
    const target = await seed(f,{type,fact:'Eligible lower-ranked workflow.'},0.90);
    await seed(f,{type,fact:'Eligible runner-up.'},0.85);
    const before = await state(f), {request,reads} = await preflight(f);
    expect(request).toMatchObject({existingMemoryId:target,existingMemoryRevision:'1',existingFact:'Eligible lower-ranked workflow.'});
    expect(reads).toHaveLength(2);
    expect(reads[0].ids).toHaveLength(10); expect(reads[0].ids.every(id => excluded.includes(id))).toBe(true);
    expect(reads[1].ids).toEqual([target]);
    expect(reads[1].values?.[9]).toBe(NOW.toISOString());
    const unwraps=kms.unwrap.mock.calls.length;
    expect(await apply(f,await decision(request!))).toEqual({action:'skipped',memoryId:target});
    expect((await pool.query('SELECT memory_id_a,memory_id_b,decision FROM contradiction_scan_log WHERE vault_id=$1',[f.vault.id])).rows)
      .toEqual([{memory_id_a:target,memory_id_b:null,decision:'merge'}]);
    expect((await state(f)).filter(r => excluded.includes(r.id))).toEqual(before.filter(r => excluded.includes(r.id)));
    expect(await state(f)).toEqual(before);
    if(encrypted) expect(kms.unwrap).toHaveBeenCalledTimes(unwraps);
  });

  it.each(['merge','discard_new','supersede_old'] as const)('applies %s to the prepared row even after a better eligible candidate appears', async action => {
    const f = await fixture();
    await seed(f,{validUntil:'2020-01-01'});
    const target = await seed(f,{},0.85), {request} = await preflight(f);
    expect(request?.existingMemoryId).toBe(target);
    const leader = await seed(f,{},0.95), before = await state(f);
    const result = await apply(f,await decision(request!,action));
    expect(result.action).toBe(action === 'supersede_old'?'inserted':'skipped');
    if(action !== 'supersede_old') expect(result.memoryId).toBe(target);
    if(action !== 'supersede_old') {
      expect((await state(f)).find(r=>r.id===target)).toEqual(before.find(r=>r.id===target));
      expect((await pool.query('SELECT memory_id_a,memory_id_b,decision FROM contradiction_scan_log WHERE vault_id=$1',[f.vault.id])).rows)
        .toEqual([{memory_id_a:target,memory_id_b:null,decision:action}]);
    }
    const after = await state(f);
    expect(after.find(r => r.id === leader)).toEqual(before.find(r => r.id === leader));
    expect(after.find(r => r.id === target)?.status).toBe(action === 'supersede_old'?'superseded':'active');
    if(action === 'supersede_old') expect((await pool.query('SELECT memory_id_a,decision FROM contradiction_scan_log WHERE vault_id=$1',[f.vault.id])).rows)
      .toEqual([{memory_id_a:target,decision:'supersede_old'}]);
  });

  it.each([false,true])('preserves exact precedence and hash-mismatched plaintext retries (encrypted=%s)', async encrypted => {
    const f = await fixture(encrypted), target = await seed(f,{},0.85);
    const {request} = await preflight(f); expect(request?.existingMemoryId).toBe(target);
    const exact = await seed(f,{fact:f.input.fact},0.9,{hash:'legacy-nonmatching-hash'});
    const before = await state(f);
    expect(await preflight(f)).toMatchObject({request:null});
    expect(await apply(f,await decision(request!,'supersede_old'))).toEqual({action:'skipped',memoryId:exact});
    expect(await state(f)).toEqual(before);
  });

  it.each([false,true])('preserves hash-based exact matches below the similarity threshold (encrypted=%s)', async encrypted => {
    const f = await fixture(encrypted);
    await seed(f,{validUntil:'2020-01-01'});
    const exact = await seed(f,{fact:f.input.fact},0.2);
    const {request,reads} = await preflight(f);
    expect(request).toBeNull(); expect(reads).toHaveLength(1); expect(reads[0].ids[0]).toBe(exact);
    expect(await apply(f)).toEqual({action:'skipped',memoryId:exact});
  });

  it('does not escalate a low-similarity false hash hint or treat the hash as plaintext equality', async () => {
    const f = await fixture(true);
    const falseHint = await seed(f,{fact:'Different plaintext with an unreliable hash.'},0.2,
      {hash:crypto.createHash('sha256').update(f.input.fact).digest('hex')});
    const target = await seed(f,{},0.85), {request,reads} = await preflight(f);
    expect(reads[0].ids[0]).toBe(falseHint); expect(reads[1].ids).toEqual([target]);
    expect(request?.existingMemoryId).toBe(target);
    expect(await apply(f,await decision(request!))).toEqual({action:'skipped',memoryId:target});
    expect((await pool.query('SELECT memory_id_a,memory_id_b,decision FROM contradiction_scan_log WHERE vault_id=$1',[f.vault.id])).rows)
      .toEqual([{memory_id_a:target,memory_id_b:null,decision:'merge'}]);
  });

  it.each(windows)('preserves historical/future behavioral exact storage and evidence union: %j', async dates => {
    const f = await fixture(true); Object.assign(f.input,dates);
    const exact = await seed(f,{fact:f.input.fact,...dates});
    expect((await preflight(f)).request).toBeNull();
    expect(await apply(f)).toEqual({action:'skipped',memoryId:exact});
    const source = crypto.randomUUID();
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role) VALUES($1,$2,'s1','user')",[source,f.vault.id]);
    f.input.sourceChunks = [source];
    expect((await apply(f)).action).toBe('inserted');
    expect((await state(f)).find(r=>r.id===exact)).toMatchObject({revision:'1',status:'active'});
    f.input.fact = 'Distinct dated workflow.';
    expect((await preflight(f)).request).toBeNull(); expect((await apply(f)).action).toBe('inserted');
    expect((await state(f)).every(r => r.status === 'active')).toBe(true);
  });

  it.each(factual.flatMap(type => windows.map(dates => ({type,dates}))))('admits historical/future factual target and input: %j', async ({type,dates}) => {
    const f = await fixture();
    await seed(f,{type:'workflow',...dates});
    const target = await seed(f,{type,...dates},0.9);
    // Unbounded current behavior can arbitrate overlapping factual history, but
    // cross-type/window model decisions cannot destructively consolidate it.
    const mixed = await preflight(f); expect(mixed.request?.existingMemoryId).toBe(target);
    Object.assign(f.input,{type,...dates});
    const {request} = await preflight(f); expect(request?.existingMemoryId).toBe(target);
    expect(await apply(f,await decision(request!))).toEqual({action:'skipped',memoryId:target});
    expect((await pool.query('SELECT memory_id_a,memory_id_b,decision FROM contradiction_scan_log WHERE vault_id=$1',[f.vault.id])).rows)
      .toEqual([{memory_id_a:target,memory_id_b:null,decision:'merge'}]);
  });

  it.each([{type:'system_fact' as const},{validFrom:'2026-09-01'},{polarity:'negative' as const}])
  ('arbitrates but preserves both for incompatible target metadata: %j', async extra => {
    const f = await fixture(); await seed(f,{validUntil:'2020-01-01'});
    const target = await seed(f,extra,0.9), {request} = await preflight(f);
    expect(request?.existingMemoryId).toBe(target);
    expect((await apply(f,await decision(request!,'supersede_old'))).action).toBe('inserted');
    expect((await state(f)).find(r => r.id === target)).toMatchObject({status:'active',revision:'1'});
  });

  it('enforces inclusive UTC endpoints and retains the preflight instant during reads', async () => {
    const f = await fixture(); Object.assign(f.input,{validFrom:'2026-09-18',validUntil:'2026-09-18'});
    const target = await seed(f,{validFrom:'2026-09-18',validUntil:'2026-09-18'},0.9);
    const reader = {query:async <T extends QueryResultRow = QueryResultRow>(sql:string,values?:unknown[]):Promise<QueryResult<T>> => {
      const result = await pool.query<T>(sql,values);
      if(sql.includes('FROM vaults')) vi.setSystemTime(new Date('2026-09-19T00:01:00Z'));
      return result;
    }};
    const request = await dedup.getDedupEscalationRequest(f.input,'test',reader,f.prepared);
    expect(request?.existingMemoryId).toBe(target);
    // Application uses its own current date, not an expired provider snapshot.
    expect((await apply(f,await decision(request!,'supersede_old'))).action).toBe('inserted');
    expect((await state(f)).find(r => r.id === target)?.status).toBe('active');
  });

  it('keeps subject, vault, exact scope binding, overlap, visibility and similarity boundaries before the eligible limit', async () => {
    const f = await fixture(true), other = await fixture(true);
    Object.assign(f.input,{validFrom:'2026-09-18',validUntil:'2026-09-18'});
    await seed(other); await seed(f,{subject:'other'}); await seed(f,{scope:'task',scopeKey:'s1'});
    await seed(f,{scopeKey:'other'}); await seed(f,{scope:'global',scopeKey:null});
    await seed(f,{type:'system_fact',validFrom:null,validUntil:'2026-09-17'}); // eligible but non-overlapping
    const archived = await seed(f), inactive = await seed(f), restricted = await seed(f), zeroConfidence = await seed(f);
    await pool.query('UPDATE memories SET archived_at=now() WHERE id=$1',[archived]);
    await pool.query("UPDATE memories SET status='superseded' WHERE id=$1",[inactive]);
    // Production rejects these invalid active states; retain that guard rather
    // than disabling triggers to manufacture unreachable dedup candidates.
    await expect(pool.query("UPDATE memories SET sensitivity='restricted' WHERE id=$1",[restricted])).rejects.toThrow('Invalid active memory');
    await expect(pool.query('UPDATE memories SET confidence=0 WHERE id=$1',[zeroConfidence])).rejects.toThrow('Invalid active memory');
    await pool.query("UPDATE memories SET status='superseded',sensitivity='restricted' WHERE id=$1",[restricted]);
    await pool.query("UPDATE memories SET status='superseded',confidence=0 WHERE id=$1",[zeroConfidence]);
    await seed(f,{type:null},1,{poison:true}); await seed(f,{},0.79);
    const target = await seed(f,{validFrom:'2026-09-18',validUntil:'2026-09-18'},0.85);
    expect((await preflight(f)).request?.existingMemoryId).toBe(target);
  });

  it('excludes malformed stored date ranges and null types before the eligible budget', async () => {
    const f = await fixture(true);
    for(const type of ['workflow','system_fact'] as const) {
      for(const dates of [{validFrom:'-infinity'},{validUntil:'infinity'},
        {validFrom:'0001-01-01 BC'},{validUntil:'10000-01-01'}]) {
        await seed(f,{type,...dates},1,{poison:true});
      }
    }
    await seed(f,{type:null},1,{poison:true});
    const target = await seed(f,{},0.85);
    const {request,reads} = await preflight(f);
    expect(request?.existingMemoryId).toBe(target);
    expect(reads[1].ids).toEqual([target]);
  });

  it('returns no escalation after one bounded eligible query when all related rows are excluded', async () => {
    const f = await fixture(true);
    for(let i=0;i<12;i++) await seed(f,windows[i%2],1,{poison:true});
    const {request,reads} = await preflight(f);
    expect(request).toBeNull(); expect(reads).toHaveLength(2);
    expect(reads[0].ids).toHaveLength(10); expect(reads[1].ids).toEqual([]);
  });

  it('uses stable UUID tie-breaking for equally ranked eligible rows', async () => {
    const f = await fixture(), ids = [crypto.randomUUID(),crypto.randomUUID()].sort();
    await seed(f,{validUntil:'2020-01-01'});
    await seed(f,{},0.85,{id:ids[1]}); await seed(f,{},0.85,{id:ids[0]});
    expect((await preflight(f)).request?.existingMemoryId).toBe(ids[0]);
  });

  it.each(['type','window','expired','future','null-type','polarity','content','scope','subject','archive','status','delete','incoming'] as const)
  ('refuses precomputed retirement after committed %s change', async change => {
    const f = await fixture(); await seed(f,{validUntil:'2020-01-01'});
    const target = await seed(f,{},0.9), {request} = await preflight(f);
    expect(request?.existingMemoryId).toBe(target);
    const changes:Record<string,string> = {
      type:"type='system_fact'",window:"valid_from='2026-09-01'",expired:"valid_until='2026-09-17'",
      future:"valid_from='2026-09-19'",'null-type':'type=NULL',polarity:"polarity='negative'",
      content:"data='Concurrently revised workflow.'",scope:"scope_key='other'",subject:"subject='other'",
      archive:'archived_at=now()',status:"status='superseded'"
    };
    if(change === 'delete') await pool.query('DELETE FROM memories WHERE id=$1',[target]);
    else if(change === 'incoming') f.input.fact = 'Different incoming statement.';
    else await pool.query(`UPDATE memories SET ${changes[change]} WHERE id=$1`,[target]);
    const before = await state(f);
    expect((await apply(f,await decision(request!,'supersede_old'))).action).toBe('inserted');
    const after = await state(f);
    expect(after.filter(r => before.some(b => b.id === r.id))).toEqual(before);
  });

  it('detects a real committed revision race between target selection and row lock', async () => {
    const f = await fixture(); await seed(f,{validUntil:'2020-01-01'});
    const target = await seed(f,{},0.9), {request} = await preflight(f);
    expect(request?.existingMemoryId).toBe(target);
    const client = await pool.connect(); let raced = false;
    try {
      await client.query('BEGIN');
      const intercepted = {query:async (sql:string,values?:unknown[]) => {
        const result = await client.query(sql,values);
        if(sql.includes('m.id=$11::uuid')) {
          expect(result.rows.map(r => r.id)).toEqual([target]);
          await pool.query("UPDATE memories SET data='Concurrent revision after target selection.' WHERE id=$1",[target]);
          raced = true;
        }
        return result;
      }} as PoolClient;
      await expect(dedup.deduplicateMemoryInTransaction(f.input,intercepted,f.prepared,[],await decision(request!,'supersede_old'))).rejects.toThrow('Memory changed during deduplication');
    } finally { await client.query('ROLLBACK'); client.release(); }
    expect(raced).toBe(true);
    expect((await state(f)).find(r => r.id === target)).toMatchObject({status:'active',revision:'2'});
    expect(await state(f)).toHaveLength(2);
  });

  it.each(['key','flag'] as const)('fails closed on changed vault crypto %s before writes', async change => {
    const f = await fixture(true); await seed(f,{validUntil:'2020-01-01'},1,{poison:true});
    await seed(f,{},0.9); const {request} = await preflight(f); expect(request).not.toBeNull();
    const before = await state(f);
    await pool.query(change === 'key' ? "UPDATE vaults SET encrypted_dek='different-synthetic-key' WHERE id=$1"
      : 'UPDATE vaults SET vault_encryption_enabled=false WHERE id=$1',[f.vault.id]);
    await expect(preflight(f)).rejects.toThrow('Prepared vault encryption identity changed');
    await expect(apply(f,await decision(request!,'supersede_old'))).rejects.toThrow('Prepared vault encryption identity changed');
    expect(await state(f)).toEqual(before);
  });

  it('leaves ordinary no-decision storage association with the historical leader unchanged', async () => {
    const f = await fixture(), old = await seed(f,{validUntil:'2020-01-01'});
    await seed(f,{},0.9);
    expect((await apply(f)).action).toBe('inserted');
    expect((await pool.query('SELECT memory_id_a,decision FROM contradiction_scan_log WHERE vault_id=$1',[f.vault.id])).rows)
      .toEqual([{memory_id_a:old,decision:'incomplete'}]);
  });
  it('refuses authenticated-but-corrupt retained ciphertext without authorizing retirement',async()=>{
    const f=await fixture(true);const target=await seed(f);const before=await state(f);
    const source=(await pool.query('SELECT blob_key,storage_bytes FROM raw_chunks WHERE id=$1',[f.input.sourceChunks[0]])).rows[0];
    const storage=(await import('../raw-chunk-storage')).getRawChunkStorage();
    const fs=await import('node:fs/promises'),path=await import('node:path');
    const {resolveRawChunkLocalRoot}=await import('../raw-chunk-storage-config');
    await fs.writeFile(path.join(resolveRawChunkLocalRoot(config.RAW_CHUNK_LOCAL_DIR),source.blob_key),'!'.repeat(Number(source.storage_bytes)));
    expect((await preflight(f)).request).toBeNull();
    expect((await apply(f)).action).toBe('inserted');
    expect((await state(f)).find(r=>r.id===target)).toEqual(before[0]);
  });

  it('does not authorize an encrypted receipt with a newly prepared replacement key identity',async()=>{
    const f=await fixture(true);await seed(f);const {request}=await preflight(f);
    const options=await decision(request!,'supersede_old');
    await pool.query("UPDATE vaults SET encrypted_dek='new-wrapped-identity' WHERE id=$1",[f.vault.id]);
    // Faithful key rewrap: test KMS returns the same DEK under a new wrapped ID.
    const replacement=await crypt.prepareVaultCrypto({...f.vault,encrypted_dek:'new-wrapped-identity'});
    await apply(f,options,replacement);
    expect((await state(f)).filter(r=>r.status==='active')).toHaveLength(2);
  });
  it.each(['global','vault'].flatMap(flag=>[false,true].map(legacy=>({flag,legacy}))))
  ('does not send ciphertext with $flag encryption disabled and legacy subject=$legacy',async({flag,legacy})=>{
    const f=await fixture(true),target=await seed(f);
    if(legacy)await pool.query("UPDATE memories SET subject='topic',subject_encrypted=NULL WHERE id=$1",[target]);
    if(flag==='global')config.ENCRYPTION_ENABLED=false;
    else await pool.query('UPDATE vaults SET vault_encryption_enabled=false WHERE id=$1',[f.vault.id]);
    try{
      const evidence=await import('../conflict-evidence');
      expect(await evidence.prepareConflictEvidence(f.vault.id,target,dedup.dedupConflictSide(f.input))).toBeNull();
    }finally{config.ENCRYPTION_ENABLED=true;}
  });
  it.each([false,true])('sends both stored entity names to the scanner without double-decryption (encrypted=%s)',async encrypted=>{
    const f=await fixture(encrypted);
    const a=await seed(f,{subject:'entity alpha',fact:'The service uses PostgreSQL.',sourceChunks:[await fixtureSource(pool,f.vault.id,'Entity alpha uses PostgreSQL.')]}),
      b=await seed(f,{subject:'entity beta',fact:'The service uses PostgreSQL.',sourceChunks:[await fixtureSource(pool,f.vault.id,'Entity beta uses PostgreSQL.')]});
    config.CONTRADICTION_SCAN_ENABLED=true;
    const {fixtureExtractor}=await import('../../__tests__/arbitration-fixtures');
    const provider=fixtureExtractor(async(_a,_b,vaultId,context)=>{
      const data=(await import('../conflict-evidence')).conflictRequestData(context.evidence,vaultId,_a,_b);
      expect([data?.A.subject,data?.B.subject].sort()).toEqual(['entity alpha','entity beta']);return 'keep_both';
    });
    await (await import('../contradiction-scanner')).scanForContradictions(f.vault.id,[b],provider as never);
    expect(provider.arbitrateConflict).toHaveBeenCalledOnce();
    expect((await state(f)).filter(r=>r.status==='active').map(r=>r.id).sort()).toEqual([a,b].sort());
  });
  it.each([false,true])('sends readable stored subjects with encrypted data (plaintext legacy subject=%s)',async legacy=>{
    const f=await fixture(true);const target=await seed(f);
    if(legacy)await pool.query("UPDATE memories SET subject='topic',subject_encrypted=NULL WHERE id=$1",[target]);
    const {request}=await preflight(f);expect(request).not.toBeNull();
    const data=(await import('../conflict-evidence')).conflictRequestData(request!.context.evidence,f.vault.id,request!.existingFact,request!.newFact);
    expect(data?.A.subject).toBe('topic');expect(data?.B.subject).toBe('topic');
  });

});
