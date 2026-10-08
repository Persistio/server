import {fixtureAssessment} from '../__tests__/arbitration-fixtures';
import crypto from 'node:crypto';
import { Client, Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ExtractedFact, ExtractorService } from '../services/extractor';

// Real processBatch -> preflight -> batched provider -> leased transaction ->
// dedup SQL, followed by the real activation drain on fresh connections. Never
// mock dedup, admission, matching, encryption, revisions, leases or scan results.
const boundary = vi.hoisted(() => ({ embed: vi.fn(), read: vi.fn(), unwrap: vi.fn(), metric: vi.fn() }));
vi.mock('node:worker_threads', async original => ({
  ...(await original() as typeof import('node:worker_threads')), parentPort: null
})); // Do not start the autonomous worker loop/claim other suites' queues.
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embedBatch: boundary.embed }) }));
vi.mock('../services/raw-chunk-storage', () => ({ getRawChunkStorage: () => ({ store: 'local', get: boundary.read, getBounded:async(key:string,max:number)=>{const raw=await boundary.read(key);if(Buffer.byteLength(raw)>max)throw new Error('Bounded fixture overflow');return raw;} }) }));
vi.mock('../services/customer-metrics', async original => ({
  ...(await original() as typeof import('../services/customer-metrics')), recordCustomerMetric: boundary.metric
}));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = boundary.unwrap; } }));

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const originalClientQuery = Client.prototype.query;
// Independent test oracle: do not import the production behavioral classification.
const behavioral = ['user_rule', 'user_preference', 'task_pattern', 'workflow', 'constraint', 'decision'] as const;
const matrix = [false, true].flatMap(encrypted => behavioral.map(type => ({ encrypted, type })));
const humanProvenance = {
  actor_type: 'human', trigger_type: 'direct', artifact_type: 'message', authorship: 'original', cadence: 'one_off',
  payload_author: { actor_type: 'human', authorship: 'original', is_user: true }
};

describe.skipIf(!databaseUrl)('extraction batch midnight preflight (PostgreSQL 17)', () => {
  const vaultIds: string[] = [], queueIds: string[] = [];
  const blobs = new Map<string, string>(), dek = Buffer.alloc(32, 59);
  const workerConnections = new Set<Client>();
  let pool: Pool | undefined;
  let db: typeof import('../db/client'), crypt: typeof import('../services/crypto');
  let processBatch: import('./extraction-execution').ExtractionExecution['processBatch'];
  let drain: typeof import('../services/contradiction-activation').drainDueContradictionActivations;
  let extractor: typeof import('../services/extractor').ExtractorService;
  let realConflict:ExtractorService['arbitrateConflict'];
  let realBatch:ExtractorService['arbitrateConflictsBatch'];
  let config: ReturnType<typeof import('../config').getConfig>;
  let activationDay: string, previousDay: string, historyDay: string, beforeMidnight: Date, afterMidnight: Date;
  let targetVector: number[], driverVector: number[], proposed: ExtractedFact[];
  const batchProvider = vi.fn<ExtractorService['arbitrateConflictsBatch']>();
  const singleProvider = vi.fn<ExtractorService['arbitrateConflict']>();

  beforeAll(async () => {
    if (!/(?:^|_)(?:test|temporal)(?:_|$)/.test(new URL(databaseUrl!).pathname.slice(1))) {
      throw new Error('Midnight worker integration requires an isolated test/temporal database');
    }
    if (process.env.DATABASE_URL !== databaseUrl) {
      throw new Error('Set DATABASE_URL and PERSISTIO_TEST_DATABASE_URL to the same isolated test database');
    }
    const url = new URL(databaseUrl!);
    url.searchParams.set('options', '-c timezone=Pacific/Honolulu -c statement_timeout=10000');
    process.env.DATABASE_URL = url.toString();
    pool = new Pool({ connectionString: url.toString() });
    db = await import('../db/client');
    await db.runMigrations();
    config = (await import('../config')).getConfig();
    config.ENCRYPTION_ENABLED = true;
    config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    config.CURATOR_AUTO_RUN = false;
    config.EXTRACTION_BATCH_SIZE = 10;
    config.EXTRACTION_WORKER_CONCURRENCY = 1;
    config.CONTRADICTION_SCAN_ENABLED = true;
    config.CONTRADICTION_SCAN_MIN_SIMILARITY = 0.8;
    config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH = 1;
    crypt = await import('../services/crypto');
    await crypt.initCryptoClient();
    extractor = (await import('../services/extractor')).ExtractorService;
    realConflict=extractor.prototype.arbitrateConflict;
    realBatch=extractor.prototype.arbitrateConflictsBatch;
    ({ processBatch } = (await import('./extraction-execution')).createExtractionExecution());
    ({ drainDueContradictionActivations: drain } = await import('../services/contradiction-activation'));
    const server = (await db.query(`SELECT current_setting('server_version_num')::int AS version,
      current_setting('TimeZone') AS timezone, (statement_timestamp() AT TIME ZONE 'UTC')::date::text AS day`)).rows[0];
    expect(server.version).toBeGreaterThanOrEqual(170000);
    expect(server.version).toBeLessThan(180000);
    expect(server.timezone).toBe('Pacific/Honolulu');
    // The activation day is the real DB day, so trigger-generated reminders are
    // already due. Fake Date only; socket/heartbeat timers and PG now() stay real.
    activationDay = server.day;
    afterMidnight = new Date(activationDay + 'T00:00:00.001Z');
    beforeMidnight = new Date(Date.parse(activationDay + 'T00:00:00Z') - 1);
    previousDay = beforeMidnight.toISOString().slice(0, 10);
    historyDay = new Date(beforeMidnight.getTime() - 86400000).toISOString().slice(0, 10);
    const column = (await pool.query(`SELECT format_type(atttypid,atttypmod) AS type
      FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'`)).rows[0].type;
    const dimensions = Number(/\((\d+)\)/.exec(column)![1]);
    expect(dimensions).toBeGreaterThan(1);
    expect(config.STORAGE_EMBEDDING_DIMENSIONS).toBe(dimensions);
    targetVector = [1, ...Array(dimensions - 1).fill(0)];
    driverVector = [0, 1, ...Array(dimensions - 2).fill(0)];
    expect((await pool.query(`SELECT tgenabled FROM pg_trigger
      WHERE tgrelid='memories'::regclass AND tgname='memories_maintain_revision'`)).rows).toEqual([{ tgenabled: 'O' }]);
  });

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(beforeMidnight);
    proposed = [];
    workerConnections.clear();
    vi.spyOn(Client.prototype, 'query').mockImplementation(function(this: Client, ...args: unknown[]) {
      if (typeof args[0] === 'string' && args[0].includes('FROM extraction_queue')) workerConnections.add(this);
      return Reflect.apply(originalClientQuery, this, args);
    } as typeof Client.prototype.query);
    boundary.unwrap.mockReset().mockResolvedValue([{ plaintext: dek }]);
    boundary.metric.mockReset();
    boundary.embed.mockReset().mockImplementation(async (texts: string[]) =>
      texts.map(text => text.toLowerCase().includes('driver') ? driverVector : targetVector));
    boundary.read.mockReset().mockImplementation(async (key: string) => {
      if (!blobs.has(key)) throw new Error('Unexpected raw blob read');
      return blobs.get(key)!;
    });
    vi.spyOn(extractor.prototype, 'extractFacts').mockImplementation(async conversation => {
      const envelope = JSON.parse(conversation);
      const source = envelope.sources.find((s: { current: boolean }) => s.current);
      expect(source.human_intent_source).toBe(true);
      return proposed.map(fact => ({ ...fact, source_refs: [source.ref] }));
    });
    vi.spyOn(extractor.prototype, 'extractSessionContext').mockResolvedValue('Explicit human workflow instructions.');
    vi.spyOn(extractor.prototype, 'extractSessionAliases').mockResolvedValue([]);
    // Unexpected tier-3 routing must fail the commit checks, not call a network.
    vi.spyOn(extractor.prototype, 'arbitrateSubject').mockRejectedValue(new Error('Unexpected subject arbitration'));
    batchProvider.mockReset().mockImplementation(async (requests,vaultId) => new Map(await Promise.all(requests.map(async r => [r.id,await fixtureAssessment(r as any,vaultId!,'keep_both',realConflict)] as const))));
    singleProvider.mockReset().mockImplementation((a,b,vaultId,context)=>fixtureAssessment({existingFact:a,newFact:b,context:context!},vaultId!,'keep_both',realConflict));
    vi.spyOn(extractor.prototype, 'arbitrateConflictsBatch').mockImplementation(batchProvider);
    vi.spyOn(extractor.prototype, 'arbitrateConflict').mockImplementation(singleProvider);
  });

  afterEach(async () => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    if (pool && vaultIds.length) {
      await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])', [queueIds]);
      await pool.query('DELETE FROM extraction_queue WHERE vault_id=ANY($1::uuid[])', [vaultIds]);
      await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaultIds]);
    }
    vaultIds.length = 0; queueIds.length = 0; blobs.clear();
  });
  afterAll(async () => { await pool?.end(); await db?.closePool(); });

  function candidate(type: ExtractedFact['type'], fact: string, subject: string,
    validFrom: string | null, validUntil: string | null): ExtractedFact {
    return { fact, subject, score: 9, salience: 0.9, sensitivity: 'low', type, scope: 'session',
      polarity: 'neutral', status: 'active', volatility: 'low', evidence: fact, scope_basis: 'Explicit session context',
      source_refs: [], valid_from: validFrom, valid_until: validUntil };
  }
  const driver = (second: boolean) => candidate('system_fact',
    `The driver database uses ${second ? 'durable queues' : 'durable tables'}.`, 'driver storage', null, null);

  async function fixture(encrypted: boolean) {
    const id = crypto.randomUUID(); vaultIds.push(id);
    const vault = { id, encrypted_dek: encrypted ? 'synthetic-midnight-key' : null, vault_encryption_enabled: encrypted };
    await pool!.query(`INSERT INTO vaults(id,name,api_key_hash,plan_id,encrypted_dek,vault_encryption_enabled)
      VALUES($1,'midnight-preflight',$2,'unlimited',$3,$4)`, [id, crypto.randomUUID(), vault.encrypted_dek, encrypted]);
    return { vault, prepared: await crypt.prepareVaultCrypto(vault), session: `midnight-${id}` };
  }
  type Fixture = Awaited<ReturnType<typeof fixture>>;
  async function enqueue(f: Fixture, facts: ExtractedFact[], replay?: { chunk: string; segment: string }) {
    const chunk = replay?.chunk ?? crypto.randomUUID(), segment = replay?.segment ?? crypto.randomUUID();
    const queue = crypto.randomUUID(), job = crypto.randomUUID(); queueIds.push(queue);
    if (!replay) {
      const key = `vaults/${f.vault.id}/sessions/${f.session}/chunks/${chunk}.txt`;
      blobs.set(key, f.prepared.encrypt(f.vault, facts.map(fact => fact.fact).join('\n')));
      // Explicit old timestamps are necessary: PostgreSQL now() is the real day,
      // and source timestamp admission must not reject a source from fake tomorrow.
      await pool!.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,
        provenance,capture_context,created_at) VALUES($1,$2,$3,'user','local',$4,$8,$5::jsonb,$6::jsonb,$7)`,
      [chunk, f.vault.id, f.session, key, JSON.stringify(humanProvenance), JSON.stringify({ session_id: f.session }),
        new Date(beforeMidnight.getTime() - 1000).toISOString(),Buffer.byteLength(blobs.get(key)!)]);
      await pool!.query(`INSERT INTO segments(id,vault_id,session_id,chunk_ids,created_at)
        VALUES($1,$2,$3,$4::uuid[],$5)`, [segment, f.vault.id, f.session, [chunk], beforeMidnight.toISOString()]);
    }
    await pool!.query("INSERT INTO jobs(id,vault_id,kind) VALUES($1,$2,'bulk_ingest')", [job, f.vault.id]);
    await pool!.query('INSERT INTO extraction_queue(id,vault_id,segment_id,job_id) VALUES($1,$2,$3,$4)',
      [queue, f.vault.id, segment, job]);
    return { chunk, segment, queue, job };
  }
  async function run(f: Fixture, facts: ExtractedFact[], replay?: { chunk: string; segment: string }) {
    const work = await enqueue(f, facts, replay);
    proposed = facts;
    await processBatch(f.vault.id);
    // processBatch catches per-job failures: prove the actual lease transaction
    // committed instead of mistaking a retry/zero-candidate path for success.
    expect((await pool!.query('SELECT retry_count,last_error FROM extraction_queue WHERE id=$1', [work.queue])).rows).toEqual([]);
    expect((await pool!.query('SELECT status FROM jobs WHERE id=$1', [work.job])).rows).toEqual([{ status: 'completed' }]);
    expect((await pool!.query('SELECT processed FROM raw_chunks WHERE id=$1', [work.chunk])).rows).toEqual([{ processed: true }]);
    expect((await pool!.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [work.queue])).rowCount).toBe(1);
    return work;
  }
  const states = async (v: string) => (await pool!.query(`SELECT id,data,subject,subject_encrypted,subject_hmac,
    hash,type,polarity,valid_from::text,valid_until::text,status,revision::text,source_chunks,evidence
    FROM memories WHERE vault_id=$1 ORDER BY id`, [v])).rows;
  const logs = async (v: string) => (await pool!.query(`SELECT memory_id_a,memory_id_b,decision,revision_a::text,revision_b::text
    FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY memory_id_a,memory_id_b`, [v])).rows;
  const schedules = async (v: string) => (await pool!.query(`SELECT memory_id,revision::text,generation,available_at,
    failures,scan_memory_id,scan_reference_date,scan_similarity FROM memory_contradiction_schedule
    WHERE vault_id=$1 ORDER BY memory_id`, [v])).rows;
  const adds = async (v: string) => Number((await pool!.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1', [v])).rows[0].memory_adds);

  function delayedBatch(expectedFacts: string[]) {
    batchProvider.mockImplementation(async (requests,vaultId) => {
      expect(new Date().toISOString()).toBe(beforeMidnight.toISOString());
      expect(requests.map(r => r.newFact).sort()).toEqual([...expectedFacts].sort());
      // Only the provider boundary moves time; neither test nor mock calls the
      // dedup preflight/apply functions. Every request was collected before this.
      await Promise.resolve();
      vi.setSystemTime(afterMidnight);
      return new Map(await Promise.all(requests.map(async r => [r.id,await fixtureAssessment(r as any,vaultId!,'keep_both',realConflict)] as const)));
    });
  }

  async function drainAll(f: Fixture, before: Awaited<ReturnType<typeof states>>) {
    const schedulerConnections = new Set<Client>();
    expect(workerConnections.size).toBeGreaterThan(0);
    const hook = vi.spyOn(Client.prototype, 'query').mockImplementation(function(this: Client, ...args: unknown[]) {
      if (typeof args[0] === 'string') {
        if (args[0].includes('SELECT memory_id, generation, failures')) schedulerConnections.add(this);
        // Same fixture clock boundary as dedup.activation.future: run untouched
        // production SQL/rows with a controlled statement time and date fence.
        // Do not fake query results, due reminders, generation or memory revisions.
        args[0] = args[0].replaceAll('statement_timestamp()', `TIMESTAMPTZ '${afterMidnight.toISOString()}'`);
      }
      return Reflect.apply(originalClientQuery, this, args);
    } as typeof Client.prototype.query);
    try {
      const scheduler = new extractor();
      // Four trigger-created reminders (target pair + orthogonal driver pair),
      // one memory per vault visit, then an empty repeat. No sleep/poll loop.
      for (let i = 0; i < before.length + 1; i++) {
        await pool!.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 minute' WHERE vault_id=$1", [f.vault.id]);
        await drain(scheduler);
      }
      expect(schedulerConnections.size).toBe(before.length);
      expect([...schedulerConnections].every(c => !workerConnections.has(c))).toBe(true);
      expect(await schedules(f.vault.id)).toEqual([]);
      expect(await states(f.vault.id)).toEqual(before);
    } finally { hook.mockRestore(); }
  }

  it.each([false,true])('commits earlier valid receipts across bounded batches when pair 17 fails (encrypted=%s)',async encrypted=>{
    const f=await fixture(encrypted);
    // Distinct names AND vectors keep the real text/vector entity resolver
    // from collapsing these independent pairs into one canonical target.
    const names=['andromeda observatory','birch orchard','copper refinery','delta reservoir',
      'emerald archive','fossil quarry','glacier outpost','helium laboratory','indigo gallery',
      'jasmine nursery','kestrel airport','lunar greenhouse','marble courthouse','neptune library',
      'orchid hospital','quartz brewery','redwood academy'];
    boundary.embed.mockImplementation(async(texts:string[])=>texts.map(text=>{
      const embedding=Array(targetVector.length).fill(0);
      const index=names.findIndex(name=>text.toLowerCase().includes(name));
      expect(index).toBeGreaterThanOrEqual(0);embedding[index]=1;return embedding;
    }));
    const old=names.map(name=>candidate('system_fact',`${name} uses PostgreSQL.`,name,null,null));
    await run(f,old);
    const next=old.map((fact,i)=>({...fact,fact:`${names[i]} correction: use SQLite.`}));
    batchProvider.mockImplementation(function(requests,vaultId){return realBatch.call(new extractor(),requests,vaultId);});
    singleProvider.mockImplementation(async(a,b,vaultId,context)=>{
      if(b===next[16].fact)throw new Error('Synthetic later partition provider failure');
      return fixtureAssessment({existingFact:a,newFact:b,context:context!},vaultId!,'supersede_old',realConflict);
    });
    await run(f,next);
    expect(batchProvider.mock.calls.map(([pairs])=>pairs.length)).toEqual([16,1]);
    const rows=await states(f.vault.id);
    expect(rows.filter(r=>r.status==='superseded')).toHaveLength(16);
    expect(rows.filter(r=>r.status==='active')).toHaveLength(18);
    expect((await logs(f.vault.id)).filter(r=>r.decision==='incomplete')).toHaveLength(1);
  });

  it.each(matrix)('defers future-at-preflight $type across provider midnight, then arbitrates unchanged revisions once (encrypted=$encrypted)',
    async ({ encrypted, type }) => {
      const f = await fixture(encrypted), v = f.vault.id;
      const first = candidate(type, 'Use the first approval workflow.', 'approval workflow', activationDay, activationDay);
      const incoming = { ...first, fact: 'Use the second approval workflow.', evidence: 'Use the second approval workflow.' };
      const seeded = await run(f, [first, driver(false)]);
      expect(batchProvider).not.toHaveBeenCalled();
      delayedBatch([driver(true).fact]);
      const applied = await run(f, [incoming, driver(true)]);
      expect(batchProvider).toHaveBeenCalledOnce();
      expect(singleProvider).not.toHaveBeenCalled();
      expect(new Date().toISOString()).toBe(afterMidnight.toISOString());
      const before = await states(v), target = before.filter(row => row.type === type);
      expect(before).toHaveLength(4); expect(target).toHaveLength(2);
      for (const [value, source] of [[first, seeded.chunk], [incoming, applied.chunk]] as const) {
        const stored = target.find(row => f.prepared.decrypt(f.vault, row.data) === value.fact)!;
        expect(stored).toMatchObject({ type, polarity: 'neutral', valid_from: activationDay, valid_until: activationDay,
          status: 'active', revision: '1', source_chunks: [source], evidence: { summary: value.evidence } });
        expect(stored.hash).toBe(crypto.createHash('sha256').update(value.fact).digest('hex'));
        if (encrypted) {
          expect(stored.data).not.toBe(value.fact); expect(stored.subject).toBe('');
          expect(f.prepared.decrypt(f.vault, stored.subject_encrypted)).toBe(first.subject);
          expect(stored.subject_hmac).toBeTruthy();
        } else expect(stored.subject).toBe(first.subject);
      }
      expect((await pool!.query('SELECT count(*)::int AS n FROM memory_embeddings WHERE memory_id=ANY($1::uuid[])',
        [before.map(row => row.id)])).rows[0].n).toBe(4);
      expect(await adds(v)).toBe(4);
      const initialSchedules = await schedules(v);
      expect(initialSchedules).toHaveLength(4);
      expect(initialSchedules.every(row => row.revision === '1' && row.failures === 0)).toBe(true);
      // P2 negative control: the unfixed apply-time-only check writes a terminal
      // keep_both for the target here, although it was excluded at preflight.
      const beforeLogs = await logs(v);
      expect(beforeLogs).toHaveLength(1); // Only the actually arbitrated driver.
      expect(beforeLogs.some(log => target.some(row => [log.memory_id_a, log.memory_id_b].includes(row.id)))).toBe(false);

      // Exact same-source replay still goes through the actual batch worker.
      // It must neither call a provider nor charge, revise, schedule or certify.
      await run(f, [incoming, driver(true)], applied);
      expect(batchProvider).toHaveBeenCalledOnce();
      expect(await states(v)).toEqual(before); expect(await logs(v)).toEqual(beforeLogs);
      expect(await schedules(v)).toEqual(initialSchedules); expect(await adds(v)).toBe(4);

      await drainAll(f, before);
      expect(singleProvider).toHaveBeenCalledOnce();
      const [existingFact, newFact, vaultId, context] = singleProvider.mock.calls[0];
      expect([existingFact, newFact].sort()).toEqual([first.fact, incoming.fact].sort());
      expect(vaultId).toBe(v);
      expect(context).toMatchObject({ existing: { validFrom: activationDay, validUntil: activationDay },
        incoming: { validFrom: activationDay, validUntil: activationDay } });
      const completed = (await logs(v)).filter(log => target.some(row => row.id === log.memory_id_a));
      expect(completed).toHaveLength(1);
      expect(completed[0]).toMatchObject({ decision: 'keep_both', revision_a: '1', revision_b: '1' });
      expect([completed[0].memory_id_a, completed[0].memory_id_b].sort()).toEqual(target.map(row => row.id).sort());
      expect(await logs(v)).toHaveLength(2); expect(await adds(v)).toBe(4);
    });

  it.each(matrix.flatMap(row => (['historical', 'current', 'expires-at-apply'] as const).map(state => ({ ...row, state }))))
  ('$state $type control preserves terminal storage semantics and apply-time authority (encrypted=$encrypted)',
    async ({ encrypted, type, state }) => {
      const f = await fixture(encrypted), v = f.vault.id;
      const from = state === 'historical' ? historyDay : previousDay;
      const until = state === 'historical' ? historyDay : state === 'current' ? activationDay : previousDay;
      const first = candidate(type, 'Use the first approval workflow.', 'approval workflow', from, until);
      const incoming = { ...first, fact: 'Use the second approval workflow.', evidence: 'Use the second approval workflow.' };
      await run(f, [first, driver(false)]);
      expect(batchProvider).not.toHaveBeenCalled();
      // Expired-at-apply requests return a destructive decision: preflight time
      // must NOT become the authority clock for applying a provider decision.
      delayedBatch(state === 'historical' ? [driver(true).fact] : [incoming.fact, driver(true).fact]);
      if (state === 'expires-at-apply') {
        const delayed = batchProvider.getMockImplementation()!;
        batchProvider.mockImplementation(async (...args) => {
          const decisions = await delayed(...args);
          for (const request of args[0]) if (request.newFact === incoming.fact) decisions.set(request.id,await fixtureAssessment(request as any,args[1]!,'supersede_old',realConflict));
          return decisions;
        });
      }
      const applied = await run(f, [incoming, driver(true)]);
      expect(batchProvider).toHaveBeenCalledOnce(); expect(singleProvider).not.toHaveBeenCalled();
      const before = await states(v), target = before.filter(row => row.type === type);
      expect(before).toHaveLength(4); expect(target).toHaveLength(2);
      expect(target.every(row => row.status === 'active' && row.revision === '1')).toBe(true);
      const completed = await logs(v);
      expect(completed).toHaveLength(2);
      expect(completed.every(log => log.revision_a === '1' && log.revision_b === '1')).toBe(true);
      expect(completed.map(log=>log.decision).sort()).toEqual(state==='current'?['keep_both','keep_both']:['incomplete','keep_both']);
      expect(completed.some(log => [log.memory_id_a, log.memory_id_b].sort().join() === target.map(row => row.id).sort().join())).toBe(true);
      await run(f, [incoming, driver(true)], applied);
      expect(batchProvider).toHaveBeenCalledOnce(); expect(await states(v)).toEqual(before);
      await drainAll(f, before);
      expect(singleProvider).not.toHaveBeenCalled();
      expect(await logs(v)).toEqual(completed); expect(await adds(v)).toBe(4);
    });
});
