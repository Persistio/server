import crypto from 'node:crypto';
import http from 'node:http';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { fixtureSource, cleanFixtureSources } from '../../__tests__/arbitration-fixtures';
import type { DedupInput } from '../dedup';
import type { ConflictResolution } from '../extractor';

const embedding = vi.hoisted(() => ({ vector: [] as number[] }));
vi.mock('../embedder', () => ({ getEmbedder: () => ({ embedBatch: async (texts: string[]) => texts.map(() => embedding.vector) }) }));
const kms = vi.hoisted(() => ({ unwrap: vi.fn() }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = kms.unwrap; } }));
const url = process.env.PERSISTIO_TEST_DATABASE_URL;
// Run with both repository database URLs set: skipped DB tests are not evidence.
describe.skipIf(!url)('Part 7-2 atomic arbitration outcomes (PostgreSQL + local SDK transport)', () => {
  const pool = new Pool({ connectionString: url });
  const vaults: string[] = [];
  const queues: string[] = [];
  let db: typeof import('../../db/client');
  let dedup: typeof import('../dedup');
  let scanner: typeof import('../contradiction-scanner');
  let model: typeof import('../extractor');
  let config: ReturnType<typeof import('../../config').getConfig>;
  let crypt: typeof import('../crypto');
  const dek = Buffer.alloc(32, 27);
  let vector: number[];
  let verdict: ConflictResolution = 'keep_both';
  let sends = 0;
  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks).toString());
    const pair = JSON.parse(body.messages[1].content);
    sends++;
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ id: 'local-outcome', object: 'chat.completion', created: 1, model: body.model, usage:{prompt_tokens:100,completion_tokens:50,total_tokens:150},
      choices: [{ index: 0, finish_reason: 'stop', message: { role: 'assistant', content: JSON.stringify({
        pairId: pair.pairId, decision: verdict,
        citationsA: [{ sourceId: pair.A.sources[0].id, quote: pair.A.sources[0].text }],
        citationsB: [{ sourceId: pair.B.sources[0].id, quote: pair.B.sources[0].text }]
      }) } }] }));
  });

  beforeAll(async () => {
    if (!url || process.env.DATABASE_URL !== url || !/test/.test(new URL(url).pathname)) {
      throw new Error('Set DATABASE_URL and PERSISTIO_TEST_DATABASE_URL to the same isolated test database');
    }
    db = await import('../../db/client');
    await db.runMigrations();
    config = (await import('../../config')).getConfig();
    config.ENCRYPTION_ENABLED = true;
    config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    crypt = await import('../crypto');
    await crypt.initCryptoClient();
    kms.unwrap.mockResolvedValue([{ plaintext: dek }]);
    config.CONTRADICTION_SCAN_ENABLED = true;
    vector = [1, ...Array(config.STORAGE_EMBEDDING_DIMENSIONS - 1).fill(0)];
    embedding.vector = vector;
    config.CURATOR_AUTO_RUN = false;
    dedup = await import('../dedup');
    scanner = await import('../contradiction-scanner');
    model = await import('../extractor');
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    config.ESCALATION_BASE_URL = `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`;
    config.ESCALATION_API_KEY = 'synthetic-local-test';
    config.ESCALATION_MODEL = 'deterministic-local';
  });
  beforeEach(() => { sends = 0; verdict = 'keep_both'; });
  afterEach(async () => {
    await removeFault();
    vi.restoreAllMocks();
    await cleanFixtureSources();
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])', [queues]);
    queues.length=0;
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]);
    vaults.length = 0;
  });
  afterAll(async () => {
    await new Promise<void>(resolve => server.close(() => resolve()));
    await pool.end();
    await db?.closePool();
  });

  async function fixture(path: 'ingestion' | 'scanner', encrypted = false) {
    const vaultId = crypto.randomUUID();
    vaults.push(vaultId);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'atomic-outcomes',$2,'unlimited')", [vaultId, crypto.randomUUID()]);
    if (encrypted) await pool.query("UPDATE vaults SET encrypted_dek='test-wrapped-dek',vault_encryption_enabled=true WHERE id=$1", [vaultId]);
    const oldText = 'Project Atlas uses PostgreSQL for durable storage.';
    const newText = verdict === 'supersede_old'
      ? 'Explicit correction: Project Atlas now uses SQLite instead of PostgreSQL.'
      : 'Confirmed: Project Atlas uses PostgreSQL for durable storage.';
    const sources = [await fixtureSource(pool, vaultId, oldText), await fixtureSource(pool, vaultId, newText)];
    const original: DedupInput = { vaultId, fact: oldText, subject: 'atlas', embedding: vector,
      sourceChunks: [sources[0]], score: 8, salience: 0.8, sensitivity: 'low', type: 'system_fact',
      scope: 'global', scopeKey: null, polarity: 'neutral', status: 'active', volatility: 'low', validFrom: null, validUntil: null };
    const first = await dedup.deduplicateMemory(original);
    const input: DedupInput = { ...original, fact: newText, sourceChunks: [sources[1]], sensitivity: 'high', sourceTimestamp: '2025-03-01T00:00:00Z' };
    let current: string | undefined;
    if (path === 'scanner') {
      // A separate supported baseline subject avoids an ingestion pair log;
      // scanner discovery is deliberately cross-subject on unchanged main.
      current = (await dedup.deduplicateMemory({ ...input, subject: 'atlas-storage' })).memoryId;
    }
    const apply = () => path === 'ingestion'
      ? dedup.deduplicateMemory(input, new model.ExtractorService())
      : scanner.scanForContradictions(vaultId, [current!], new model.ExtractorService(), { budget: { remaining: 1 } });
    return { vaultId, sources, first: first.memoryId!, input, current, apply };
  }

  async function snapshot(vaultId: string) {
    const memories = (await pool.query('SELECT * FROM memories WHERE vault_id=$1 ORDER BY id', [vaultId])).rows;
    const embeddings = (await pool.query('SELECT e.* FROM memory_embeddings e JOIN memories m ON m.id=e.memory_id WHERE m.vault_id=$1 ORDER BY e.memory_id', [vaultId])).rows;
    const audit = (await pool.query('SELECT * FROM memory_mutation_events WHERE vault_id=$1 ORDER BY id', [vaultId])).rows;
    const outcomes = (await pool.query('SELECT * FROM contradiction_scan_log WHERE vault_id=$1 AND send_attempt IS NULL ORDER BY id', [vaultId])).rows;
    const quota = (await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1', [vaultId])).rows;
    const queue = (await pool.query('SELECT * FROM curation_queue WHERE vault_id=$1 ORDER BY id', [vaultId])).rows;
    const targets = (await pool.query('SELECT * FROM curation_queue_items WHERE vault_id=$1 ORDER BY queue_id,memory_id', [vaultId])).rows;
    return { memories, embeddings, audit, outcomes, quota, queue, targets };
  }
  async function removeFault() {
    for (const table of ['contradiction_scan_log','memories','memory_embeddings','curation_queue_items','vault_usage']) {
      await pool.query(`DROP TRIGGER IF EXISTS part72_test_audit_failure ON ${table}`);
    }
    await pool.query('DROP FUNCTION IF EXISTS part72_test_audit_failure()');
  }
  async function failOutcomeAudit(vaultId: string) {
    // Generated UUID only; the fault does not intercept pre-send reservations.
    await pool.query(`CREATE FUNCTION part72_test_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.vault_id='${vaultId}'::uuid AND NEW.send_attempt IS NULL THEN
        RAISE EXCEPTION 'synthetic outcome audit failure'; END IF; RETURN NEW; END $$`);
    await pool.query('CREATE TRIGGER part72_test_audit_failure BEFORE INSERT ON contradiction_scan_log FOR EACH ROW EXECUTE FUNCTION part72_test_audit_failure()');
  }

  it.each(['memories','memory_embeddings','curation_queue_items','vault_usage'] as const)('rolls back replacement on late %s failure', async table => {
    verdict = 'supersede_old';
    const f = await fixture('ingestion'), before = await snapshot(f.vaultId);
    const condition = table === 'memory_embeddings'
      ? `EXISTS(SELECT 1 FROM memories WHERE id=NEW.memory_id AND vault_id='${f.vaultId}'::uuid)`
      : `NEW.vault_id='${f.vaultId}'::uuid`;
    await pool.query(`CREATE FUNCTION part72_test_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF ${condition} THEN RAISE EXCEPTION 'synthetic write failure'; END IF; RETURN NEW; END $$`);
    await pool.query(`CREATE TRIGGER part72_test_audit_failure BEFORE INSERT ON ${table} FOR EACH ROW EXECUTE FUNCTION part72_test_audit_failure()`);
    await expect(f.apply()).rejects.toThrow('synthetic write failure');
    expect(await snapshot(f.vaultId)).toEqual(before);
    expect(sends).toBe(1);
  });

  it('suppresses replay after a lost successful COMMIT acknowledgment', async () => {
    verdict = 'merge';
    const f = await fixture('ingestion');
    const request = (await dedup.getDedupEscalationRequest(f.input,'test'))!;
    const assessment = await new model.ExtractorService().arbitrateConflict(request.existingFact,request.newFact,f.vaultId,request.context);
    const transaction = db.withTransaction;
    vi.spyOn(db,'withTransaction').mockImplementationOnce(async work => {
      await transaction(work);
      throw new Error('lost COMMIT acknowledgment');
    });
    await expect(dedup.deduplicateMemory(f.input,undefined,{precomputedConflictDecision:assessment,
      precomputedConflictInput:request.inputFingerprint,precomputedConflictMemoryId:request.existingMemoryId,
      precomputedConflictMemoryRevision:request.existingMemoryRevision})).rejects.toThrow('lost COMMIT');
    const committed = await snapshot(f.vaultId);
    await f.apply();
    expect(sends).toBe(1);expect(await snapshot(f.vaultId)).toEqual(committed);
  });

  it('serializes concurrent applications of one genuine outcome without duplicate committed effects', async () => {
    verdict = 'merge';const f = await fixture('ingestion');
    const request = (await dedup.getDedupEscalationRequest(f.input,'test'))!;
    const assessment = await new model.ExtractorService().arbitrateConflict(request.existingFact,request.newFact,f.vaultId,request.context);
    const options = {precomputedConflictDecision:assessment,precomputedConflictInput:request.inputFingerprint,
      precomputedConflictMemoryId:request.existingMemoryId,precomputedConflictMemoryRevision:request.existingMemoryRevision};
    const results = await Promise.all(Array.from({length:4},() => dedup.deduplicateMemory(f.input,undefined,options)));
    expect(results.filter(r=>r.action==='updated')).toHaveLength(1);
    expect(results.filter(r=>r.action==='skipped')).toHaveLength(3);
    expect((await snapshot(f.vaultId)).outcomes).toHaveLength(1);expect(sends).toBe(1);
  });

  it('never resets the three-send allowance when every authorized apply rolls back', async () => {
    verdict='supersede_old';const f=await fixture('ingestion'),before=await snapshot(f.vaultId);
    await failOutcomeAudit(f.vaultId);
    for(let attempt=1;attempt<=4;attempt++) {
      await expect(f.apply()).rejects.toThrow('synthetic outcome audit failure');
      expect(await snapshot(f.vaultId)).toEqual(before);
      expect(sends).toBe(Math.min(attempt,3));
    }
    expect((await pool.query('SELECT send_attempt FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY send_attempt',[f.vaultId])).rows)
      .toEqual([{send_attempt:1},{send_attempt:2},{send_attempt:3}]);
  });

  it.each((['ingestion','scanner'] as const).flatMap(path => (['discard_new','merge'] as const).map(decision=>({path,decision}))))('set-equivalent support preserves the survivor regardless of array order: %j',async ({path,decision})=>{
    verdict=decision;const f=await fixture(path);
    // A already has all support in reverse UUID order. B's strict subset adds
    // no evidence. Both paths must compare sets rather than the sorted union.
    const unsorted=[...f.sources].sort().reverse();
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1',[f.first,unsorted]);
    f.input.sourceChunks=[f.sources[0]];f.input.sensitivity='low';f.input.sourceTimestamp=null;
    if(f.current)await pool.query("UPDATE memories SET source_chunks=$2::uuid[],sensitivity='low',source_timestamp=NULL WHERE id=$1",[f.current,[f.sources[0]]]);
    const before=await snapshot(f.vaultId);await f.apply();const after=await snapshot(f.vaultId);
    expect(after.memories.find(m=>m.id===f.first)).toEqual(before.memories.find(m=>m.id===f.first));
    expect(after.queue).toEqual(before.queue);expect(after.targets).toEqual(before.targets);
    expect(after.outcomes.map(row=>row.decision)).toEqual([decision]);
    await f.apply();expect(sends).toBe(1);expect(await snapshot(f.vaultId)).toEqual(after);
  });

  it.each(['keep_both','supersede_old','discard_new','merge'] as const)('actual extraction worker %s rolls back receipt/completion and commits a fresh authorized retry once', async decision => {
    verdict=decision;const f=await fixture('ingestion');
    const segment=crypto.randomUUID(),queue=crypto.randomUUID();queues.push(queue);
    await pool.query("INSERT INTO segments(id,vault_id,session_id,chunk_ids) VALUES($1,$2,'s1',$3::uuid[])",[segment,f.vaultId,[f.sources[1]]]);
    await pool.query('INSERT INTO extraction_queue(id,vault_id,segment_id) VALUES($1,$2,$3)',[queue,f.vaultId,segment]);
    vi.spyOn(model.ExtractorService.prototype,'extractFacts').mockImplementation(async conversation=>{
      const data=JSON.parse(conversation);
      return [{fact:f.input.fact,subject:'atlas',score:8,salience:0.8,sensitivity:'high',type:'system_fact',
        scope:'global',polarity:'neutral',volatility:'low',evidence:'Supported storage fact',
        source_refs:[data.sources.find((source:any)=>source.current).ref],scope_basis:'Global system fact',valid_from:null,valid_until:null}] as never;
    });
    vi.spyOn(model.ExtractorService.prototype,'extractSessionContext').mockResolvedValue('Storage context');
    vi.spyOn(model.ExtractorService.prototype,'extractSessionAliases').mockResolvedValue([]);
    const {processBatch}=await import('../../daemon/extraction-worker');
    const before=await snapshot(f.vaultId);
    await failOutcomeAudit(f.vaultId);await processBatch(f.vaultId);
    expect(sends).toBe(1);expect(await snapshot(f.vaultId)).toEqual(before);
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1',[queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT processed FROM raw_chunks WHERE id=$1',[f.sources[1]])).rows[0].processed).toBe(false);
    await removeFault();await processBatch(f.vaultId);
    expect(sends).toBe(2);
    const after=await snapshot(f.vaultId);
    expect(after.outcomes.map(row=>row.decision)).toEqual([decision]);
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1',[queue])).rowCount).toBe(1);
    expect((await pool.query('SELECT 1 FROM extraction_queue WHERE id=$1',[queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT processed FROM raw_chunks WHERE id=$1',[f.sources[1]])).rows[0].processed).toBe(true);
    await processBatch(f.vaultId);expect(sends).toBe(2);expect(await snapshot(f.vaultId)).toEqual(after);
  });

  for (const path of ['ingestion', 'scanner'] as const) {
    it.each([false,true].flatMap(encrypted => (['keep_both', 'supersede_old', 'discard_new', 'merge'] as const).map(decision => ({encrypted,decision}))))(`${path}: commits truthful outcome, support and exact replay %j`, async ({decision,encrypted}) => {
      verdict = decision;
      const f = await fixture(path,encrypted);
      const before = await snapshot(f.vaultId);
      await f.apply();
      const state = await snapshot(f.vaultId);
      const active = state.memories.filter(m => m.status === 'active' && m.archived_at === null);
      expect(sends).toBe(1);
      expect(active).toHaveLength(decision === 'keep_both' ? 2 : 1);
      const old = state.memories.find(m => m.id === f.first)!;
      if (decision === 'supersede_old') expect(old.status).toBe('superseded');
      if (decision === 'discard_new' || decision === 'merge') {
        expect(old.data).toBe(before.memories.find(m => m.id === f.first).data);
        expect(old.sensitivity).toBe('high');
        expect(old.source_timestamp.toISOString()).toBe('2025-03-01T00:00:00.000Z');
        expect(state.embeddings.find(e => e.memory_id === f.first)).toEqual(before.embeddings.find(e => e.memory_id === f.first));
        expect(state.targets.some(t => t.memory_id === old.id && t.revision === old.revision)).toBe(true);
      }
      if (decision === 'supersede_old') expect(active[0].source_chunks).toEqual([f.sources[1]]);
      if (encrypted) expect(active.every(m => m.data !== f.input.fact)).toBe(true);
      expect(state.quota[0].memory_adds - before.quota[0].memory_adds).toBe(path === 'ingestion' && (decision === 'keep_both' || decision === 'supersede_old') ? 1 : 0);
      if (decision === 'merge' || decision === 'discard_new') expect([...old.source_chunks].sort()).toEqual([...f.sources].sort());
      expect(state.outcomes.map(row => row.decision)).toEqual([decision]);
      expect(state.embeddings.filter(e => active.some(m => m.id === e.memory_id))).toHaveLength(active.length);
      for (const memory of active) expect(state.embeddings.find(e => e.memory_id === memory.id).embedding).toEqual(memory.embedding);
      await f.apply();
      expect(sends).toBe(1);
      expect(await snapshot(f.vaultId)).toEqual(state);
    });

    it.each(['keep_both','supersede_old','discard_new','merge'] as const)(`${path}: %s rollback permits a fresh authorized send within the unchanged allowance`, async decision => {
      verdict = decision;
      const f = await fixture(path);
      const before = await snapshot(f.vaultId);
      await failOutcomeAudit(f.vaultId);
      await expect(f.apply()).rejects.toThrow('synthetic outcome audit failure');
      expect(await snapshot(f.vaultId)).toEqual(before);
      expect(sends).toBe(1);
      expect((await pool.query('SELECT decision,send_attempt FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY send_attempt', [f.vaultId])).rows)
        .toEqual([{ decision: 'incomplete', send_attempt: 1 }]);
      await removeFault();
      await f.apply();
      const after = await snapshot(f.vaultId);
      expect(after.memories.filter(m => m.status === 'active')).toHaveLength(decision === 'keep_both' ? 2 : 1);
      expect(after.outcomes.map(row => row.decision)).toEqual([decision]);
      // Rollback permits a fresh send; it does not recover a process-local receipt.
      expect(sends).toBe(2);
      expect((await pool.query("SELECT sum(request_count)::int AS requests,sum(total_tokens)::int AS tokens FROM vault_model_usage WHERE vault_id=$1 AND model_role='escalation'",[f.vaultId])).rows[0]).toEqual({requests:2,tokens:300});
      expect((await pool.query('SELECT send_attempt FROM contradiction_scan_log WHERE vault_id=$1 AND send_attempt IS NOT NULL ORDER BY send_attempt', [f.vaultId])).rows)
        .toEqual([{send_attempt:1},{send_attempt:2}]);
      await f.apply();
      expect(sends).toBe(2);
      expect(await snapshot(f.vaultId)).toEqual(after);
    });
  }
});
