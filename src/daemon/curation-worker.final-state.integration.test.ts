import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import type { CuratorProposedMemory, CuratorResult } from '../services/curator-contract';

// Only the external model, embedding provider and KMS are replaced. All SQL,
// lease/receipt/accounting, AES-GCM and processBatch application paths are real.
const state = vi.hoisted(() => ({ embed: vi.fn(), unwrap: vi.fn() }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed: state.embed }) }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = state.unwrap; } }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
type PresentedMemory = { id: string; statement: string; subject: string };
type Fixture = { vault: string; queue: string; ids: string[] };

describe.skipIf(!databaseUrl)('curator final-state survivor invariants (real PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [], queues: string[] = [];
  let db: typeof import('../db/client');
  let worker: typeof import('./curation-worker');
  let CuratorService: typeof import('../services/curator').CuratorService;
  let config: ReturnType<typeof import('../config').getConfig>;
  let vector: number[];
  let rawRoot: string;

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    process.env.CURATOR_API_KEY = 'synthetic-final-state-provider';
    db = await import('../db/client');
    await db.runMigrations();
    config = (await import('../config')).getConfig();
    config.ENCRYPTION_ENABLED = false;
    config.CURATION_BATCH_SIZE = 1;
    rawRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'curator-final-state-sources-'));
    config.RAW_CHUNK_STORAGE_PROVIDER = 'local';
    config.RAW_CHUNK_LOCAL_DIR = rawRoot;
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    vector = [1, ...Array(Number(/\((\d+)\)/.exec(type)![1]) - 1).fill(0)];
    state.embed.mockResolvedValue(vector);
    ({ CuratorService } = await import('../services/curator'));
    worker = await import('./curation-worker');
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    config.ENCRYPTION_ENABLED = false;
    state.embed.mockReset().mockResolvedValue(vector);
    state.unwrap.mockReset();
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])', [queues]);
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]);
    vaults.length = 0; queues.length = 0;
  });
  afterAll(async () => { await pool.end(); await db?.closePool(); if (rawRoot) await fs.rm(rawRoot, { recursive: true, force: true }); });

  async function addMemory(vault: string, statement: string, subject = 'Test topic') {
    const id = crypto.randomUUID();
    await pool.query(`INSERT INTO memories(id,vault_id,data,subject,hash,scope,scope_key,status,type,confidence,salience,embedding)
      VALUES($1,$2,$3,$4,$5,'session','session1','active','system_fact',0.9,0.8,$6::vector)`,
    [id, vault, statement, subject, crypto.randomUUID(), JSON.stringify(vector)]);
    await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)', [id, JSON.stringify(vector)]);
    return id;
  }
  async function fixture(count = 2): Promise<Fixture> {
    const vault = crypto.randomUUID(), queue = crypto.randomUUID();
    vaults.push(vault); queues.push(queue);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'final-state-regression',$2,'unlimited')", [vault, crypto.randomUUID()]);
    const ids: string[] = [];
    for (let i = 0; i < count; i++) ids.push(await addMemory(vault, `Original fact ${i}`));
    await pool.query('INSERT INTO curation_queue(id,vault_id,work_key) VALUES($1,$2,$3)', [queue, vault, crypto.randomUUID()]);
    await pool.query('INSERT INTO curation_queue_items(queue_id,vault_id,memory_id,revision) SELECT $1,vault_id,id,revision FROM memories WHERE id=ANY($2::uuid[])', [queue, ids]);
    return { vault, queue, ids };
  }
  const empty = (): CuratorResult => ({ schema_version: 'curation-plan.v2', keep: [], update: [], consolidate: [], archive: [], edges: [], scope_changes: [] });
  const memory = (statement = 'Final duplicate fact', subject = 'Test topic'): CuratorProposedMemory => ({ statement, subject, type: 'system_fact', confidence: 0.9, salience: 0.8,
    sensitivity: 'low', polarity: 'neutral', volatility: 'low', valid_from: null, valid_until: null, evidence: 'Supported by reviewed facts' });
  const update = (id: string, proposed = memory()) => ({ id, memory: proposed, source_refs: [id], reason: 'Supported refinement' });
  const consolidate = (id: string, sources: string[], proposed = memory()) => ({ id, sources, memory: proposed, reason: 'Supported consolidation' });
  function model(fn: (targets: PresentedMemory[], context: PresentedMemory[], sources: Array<{ id: string; content: string }>) => unknown | Promise<unknown>) {
    return vi.spyOn(CuratorService.prototype as any, 'createChatCompletion').mockImplementation(async (request: any, _vault: any, accounting: any) => {
      await accounting.beforeRequest();
      const parts = request.messages[1].content;
      const result = await fn(JSON.parse(parts[0].text).memories, JSON.parse(parts[1].text).memories, JSON.parse(parts[2].text).sources);
      await accounting.returnedUsage({ promptTokens: 100, completionTokens: 50, totalTokens: 150 });
      return { usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 }, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(result) } }] };
    });
  }
  async function semanticState(vault: string) {
    return {
      memories: (await pool.query('SELECT * FROM memories WHERE vault_id=$1 ORDER BY id', [vault])).rows,
      embeddings: (await pool.query('SELECT e.* FROM memory_embeddings e JOIN memories m ON m.id=e.memory_id WHERE m.vault_id=$1 ORDER BY e.memory_id', [vault])).rows,
      edges: (await pool.query('SELECT * FROM memory_edges WHERE vault_id=$1 ORDER BY id', [vault])).rows,
      actions: (await pool.query('SELECT * FROM curation_action_log WHERE vault_id=$1 ORDER BY id', [vault])).rows,
      mutations: (await pool.query('SELECT * FROM memory_mutation_events WHERE vault_id=$1 ORDER BY id', [vault])).rows,
      scopes: (await pool.query('SELECT * FROM memory_scope_change_log WHERE vault_id=$1 ORDER BY id', [vault])).rows,
      activation: (await pool.query('SELECT * FROM memory_contradiction_schedule WHERE vault_id=$1 ORDER BY memory_id', [vault])).rows,
    };
  }
  async function assertRejected(f: Fixture, before: Awaited<ReturnType<typeof semanticState>>, reviewStatuses = ['application_failed']) {
    expect(await semanticState(f.vault)).toEqual(before);
    expect((await pool.query("SELECT * FROM worker_action_receipts WHERE queue_id=$1 AND action_key LIKE 'apply-actions:%'", [f.queue])).rows).toEqual([]);
    expect((await pool.query('SELECT curator_requests,curator_input_tokens,curator_output_tokens,curator_candidates_processed FROM vault_usage WHERE vault_id=$1', [f.vault])).rows)
      .toEqual([{ curator_requests: 1, curator_input_tokens: 100, curator_output_tokens: 50, curator_candidates_processed: 0 }]);
    const reviews = (await pool.query('SELECT validation_status,after_state,applied_at FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows;
    expect(reviews).toHaveLength(1);
    expect(reviewStatuses).toContain(reviews[0].validation_status);
    expect(reviews[0]).toMatchObject({ after_state: null, applied_at: null });
    expect((await pool.query('SELECT memory_id FROM curation_queue_items WHERE queue_id=$1 ORDER BY memory_id', [f.queue])).rows.map(r => r.memory_id)).toEqual([...f.ids].sort());
  }

  it.each(['update/update', 'consolidate/consolidate', 'consolidate/update'] as const)('baseline: rejects duplicate final survivors from %s atomically', async kind => {
    const f = await fixture(kind === 'consolidate/consolidate' ? 4 : kind === 'consolidate/update' ? 3 : 2);
    const before = await semanticState(f.vault);
    const call = model(targets => {
      const ids = targets.map(t => t.id);
      return kind === 'update/update' ? { ...empty(), update: ids.map(id => update(id)) }
        : kind === 'consolidate/consolidate' ? { ...empty(), consolidate: [consolidate('N1', ids.slice(0, 2)), consolidate('N2', ids.slice(2))] }
        : { ...empty(), consolidate: [consolidate('N1', ids.slice(0, 2))], update: [update(ids[2])] };
    });
    await worker.processBatch();
    expect(call).toHaveBeenCalledOnce();
    expect(state.embed).toHaveBeenCalledTimes(2);
    await assertRejected(f, before);
  });

  async function syncQueue(f: Fixture) {
    await pool.query('UPDATE curation_queue_items i SET revision=m.revision FROM memories m WHERE m.id=i.memory_id AND i.queue_id=$1', [f.queue]);
  }
  async function assertApplied(f: Fixture) {
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows).toEqual([{ validation_status: 'applied' }]);
    expect((await pool.query('SELECT id FROM curation_queue WHERE id=$1', [f.queue])).rows).toEqual([]);
    expect((await pool.query('SELECT curator_candidates_processed FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0].curator_candidates_processed).toBe(f.ids.length);
    expect((await pool.query("SELECT accounted_at IS NOT NULL AS accounted FROM worker_action_receipts WHERE queue_id=$1 AND action_key LIKE 'apply-actions:%'", [f.queue])).rows).toEqual([{ accounted: true }]);
  }
  async function source(vault: string) {
    const id = crypto.randomUUID();
    // A remote optional blob is deliberately not read; lineage ownership is SQL,
    // not a requirement to fabricate a readable source or mock blob storage.
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes) VALUES($1::uuid,$2,'session1','user','gcs',$1::text,20)", [id, vault]);
    return id;
  }

  it.each(['keep', 'update', 'consolidate'] as const)('rejects %s against a genuinely unseen active duplicate without stealing its lineage', async kind => {
    const f = await fixture(kind === 'consolidate' ? 2 : 1);
    const statement = kind === 'keep' ? 'Original fact 0' : memory().statement;
    const unseen = await addMemory(f.vault, statement, 'the test   topic');
    await pool.query('DELETE FROM memory_embeddings WHERE memory_id=$1', [unseen]);
    const ownSource = await source(f.vault), unseenSource = await source(f.vault);
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1', [f.ids[0], [ownSource]]);
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[],evidence=$3::jsonb WHERE id=$1', [unseen, [unseenSource], JSON.stringify({ unseen_lineage: true })]);
    await syncQueue(f);
    const before = await semanticState(f.vault);
    const call = model((targets, context) => {
      expect(context).toEqual([]);
      const ids = targets.map(t => t.id);
      return kind === 'keep' ? { ...empty(), keep: ids.map(id => ({ id, reason: 'Useful' })) }
        : kind === 'update' ? { ...empty(), update: [update(ids[0])] }
        : { ...empty(), consolidate: [consolidate('N1', ids)] };
    });
    await worker.processBatch(); expect(call).toHaveBeenCalledOnce();
    await assertRejected(f, before);
  });

  it('permits a collision whose other reviewed target is archived later in the same plan', async () => {
    const f = await fixture();
    model(targets => ({ ...empty(), update: [update(targets[0].id, memory(targets[1].statement))],
      archive: [{ id: targets[1].id, basis: 'duplicate', reason: 'Redundant after refinement' }] }));
    await worker.processBatch(); await assertApplied(f);
    expect((await pool.query("SELECT data FROM memories WHERE vault_id=$1 AND status='active' AND archived_at IS NULL", [f.vault])).rows).toEqual([{ data: 'Original fact 1' }]);
  });

  it.each([
    ['swap', [0, 1]], ['swap', [1, 0]],
    ['chain', [0, 1, 2]], ['chain', [0, 2, 1]], ['chain', [1, 0, 2]],
    ['chain', [1, 2, 0]], ['chain', [2, 0, 1]], ['chain', [2, 1, 0]],
  ] as const)('allows final-unique %s with update permutation %j', async (kind, order) => {
    const f = await fixture(order.length);
    model(targets => {
      const sorted = [...targets].sort((a, b) => a.statement.localeCompare(b.statement));
      return { ...empty(), update: order.map(i => update(sorted[i].id, memory(i === sorted.length - 1
        ? kind === 'swap' ? sorted[0].statement : 'Fresh terminal fact' : sorted[i + 1].statement))) };
    });
    await worker.processBatch(); await assertApplied(f);
    const rows = (await pool.query('SELECT id,data,status FROM memories WHERE vault_id=$1', [f.vault])).rows;
    expect(rows).toHaveLength(f.ids.length);
    for (let i = 0; i < f.ids.length; i++) expect(rows.find(r => r.id === f.ids[i])).toMatchObject({ status: 'active',
      data: i === f.ids.length - 1 ? kind === 'swap' ? 'Original fact 0' : 'Fresh terminal fact' : `Original fact ${i + 1}` });
  });

  it.each(['vault', 'scope', 'binding', 'type', 'polarity', 'start', 'end', 'subject', 'statement', 'archived'] as const)
  ('does not equate rows differing in exact %s despite identical hash hints', async difference => {
    const f = await fixture(1);
    const otherVault = difference === 'vault' ? await fixture(0) : null;
    if (otherVault) await pool.query('DELETE FROM curation_queue WHERE id=$1', [otherVault.queue]);
    const other = await addMemory(otherVault?.vault ?? f.vault, difference === 'statement' ? 'Different statement' : memory().statement,
      difference === 'subject' ? 'Different subject' : 'Test topic');
    await pool.query('UPDATE memories SET hash=$2 WHERE id=$1', [other, crypto.createHash('sha256').update(memory().statement).digest('hex')]);
    if (difference === 'scope') await pool.query("UPDATE memories SET scope='project' WHERE id=$1", [other]);
    if (difference === 'binding') await pool.query("UPDATE memories SET scope_key='session2' WHERE id=$1", [other]);
    if (difference === 'type') await pool.query("UPDATE memories SET type='project' WHERE id=$1", [other]);
    if (difference === 'polarity') await pool.query("UPDATE memories SET polarity='negative' WHERE id=$1", [other]);
    if (difference === 'start') await pool.query("UPDATE memories SET valid_from='2020-01-01' WHERE id=$1", [other]);
    if (difference === 'end') await pool.query("UPDATE memories SET valid_until='2020-01-01' WHERE id=$1", [other]);
    if (difference === 'archived') await pool.query("UPDATE memories SET status='superseded',archived_at=now() WHERE id=$1", [other]);
    const before = (await pool.query('SELECT * FROM memories WHERE id=$1', [other])).rows[0];
    model(targets => ({ ...empty(), update: [update(targets[0].id)] }));
    await worker.processBatch(); await assertApplied(f);
    expect((await pool.query('SELECT * FROM memories WHERE id=$1', [other])).rows[0]).toEqual(before);
    expect((await pool.query('SELECT data FROM memories WHERE id=$1', [f.ids[0]])).rows[0].data).toBe(memory().statement);
  });

  it('resolves canonical aliases inside the exact binding, not just subject spelling or stored hash', async () => {
    const f = await fixture(1), other = await addMemory(f.vault, memory().statement, 'legacy topic');
    await pool.query("INSERT INTO entity_aliases(vault_id,alias,canonical,scope,scope_key) VALUES($1,'legacy topic','test topic','session','session1')", [f.vault]);
    await pool.query('DELETE FROM memory_embeddings WHERE memory_id=$1', [other]);
    const before = await semanticState(f.vault);
    model((targets, context) => { expect(context).toEqual([]); return { ...empty(), update: [update(targets[0].id)] }; });
    await worker.processBatch(); await assertRejected(f, before);
  });

  it.each(['provider', 'embedding'] as const)('rejects a duplicate committed by another connection during %s work', async phase => {
    const f = await fixture(1); let before!: Awaited<ReturnType<typeof semanticState>>;
    const inject = async () => {
      await db.withTransaction(async client => {
        await client.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE NOWAIT', [f.vault]);
        await client.query(`INSERT INTO memories(vault_id,data,subject,hash,scope,scope_key,status,type,confidence)
          VALUES($1,$2,'Test topic','deliberately-stale-hash','session','session1','active','system_fact',0.9)`, [f.vault, memory().statement]);
      });
      before = await semanticState(f.vault);
    };
    model(async targets => { if (phase === 'provider') await inject(); return { ...empty(), update: [update(targets[0].id)] }; });
    if (phase === 'embedding') state.embed.mockImplementationOnce(async () => { await inject(); return vector; });
    await worker.processBatch(); expect(before).toBeDefined(); await assertRejected(f, before);
  });

  it.each(['provider', 'embedding'] as const)('retains independently changed reviewed revisions during %s preparation', async phase => {
    const f = await fixture(1); let before!: Awaited<ReturnType<typeof semanticState>>;
    const inject = async () => {
      await pool.query("UPDATE memories SET data='Independent correction' WHERE id=$1", [f.ids[0]]);
      before = await semanticState(f.vault);
    };
    model(async targets => { if (phase === 'provider') await inject(); return { ...empty(), update: [update(targets[0].id)] }; });
    if (phase === 'embedding') state.embed.mockImplementationOnce(async () => { await inject(); return vector; });
    await worker.processBatch(); await assertRejected(f, before);
  });

  it.each(['duplicate', 'different-plaintext'] as const)('uses actual authenticated AES-GCM plaintext equality: %s', async kind => {
    const f = await fixture(1), other = await addMemory(f.vault, memory().statement);
    const crypt = await import('../services/crypto'), key = Buffer.alloc(32, 23);
    config.ENCRYPTION_ENABLED = true; config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/final-state';
    state.unwrap.mockImplementation(async () => {
      const probe = await pool.connect();
      try {
        await probe.query('BEGIN');
        await probe.query('SELECT id FROM vaults WHERE id=$1 FOR UPDATE NOWAIT', [f.vault]);
        await probe.query('SELECT id FROM memories WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
      } finally { await probe.query('ROLLBACK'); probe.release(); }
      return [{ plaintext: key }];
    });
    await crypt.initCryptoClient();
    await pool.query('UPDATE vaults SET encrypted_dek=$2,vault_encryption_enabled=true WHERE id=$1', [f.vault, Buffer.from('synthetic-wrapped-final-state').toString('base64')]);
    const otherText = kind === 'duplicate' ? memory().statement : 'Not the same encrypted fact';
    for (const [id, text] of [[f.ids[0], 'Original fact 0'], [other, otherText]]) {
      await pool.query("UPDATE memories SET data=$2,subject='',subject_encrypted=$3,subject_hmac=$4,hash=$5 WHERE id=$1",
        [id, crypt.encryptField(text, key), crypt.encryptField('Test topic', key), crypt.computeSubjectHmac('Test topic', key),
          crypto.createHash('sha256').update(memory().statement).digest('hex')]);
    }
    await syncQueue(f);
    const ciphertext = (await pool.query('SELECT data FROM memories WHERE id=$1', [other])).rows[0].data;
    expect(ciphertext).not.toBe(crypt.encryptField(otherText, key)); // independent nonces
    expect(crypt.decryptField(ciphertext, key)).toBe(otherText);
    const before = await semanticState(f.vault);
    model(targets => ({ ...empty(), update: [update(targets[0].id)] }));
    await worker.processBatch();
    if (kind === 'duplicate') await assertRejected(f, before);
    else {
      await assertApplied(f);
      const stored = (await pool.query('SELECT data FROM memories WHERE id=$1', [f.ids[0]])).rows[0].data;
      expect(crypt.decryptField(stored, key)).toBe(memory().statement);
    }
    expect(state.unwrap).toHaveBeenCalled();
    const audit = (await pool.query('SELECT raw_response,before_state,after_state FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(audit.raw_response).toHaveProperty('encrypted');
    expect(JSON.stringify(audit)).not.toContain(memory().statement);
  });

  it('preserves source union and rewires children and evidence edges after a unique consolidation', async () => {
    const f = await fixture(3), chunks = [await source(f.vault), await source(f.vault)];
    for (let i = 0; i < 2; i++) await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1', [f.ids[i], [chunks[i]]]);
    await pool.query('UPDATE memories SET parent_id=$2 WHERE id=$1', [f.ids[2], f.ids[0]]);
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'supports',0.7,'Historic support')", [f.vault, f.ids[0], f.ids[2]]);
    await syncQueue(f);
    model(targets => {
      const sorted = [...targets].sort((a, b) => a.statement.localeCompare(b.statement));
      return { ...empty(), consolidate: [consolidate('N1', sorted.slice(0, 2).map(t => t.id))], keep: [{ id: sorted[2].id, reason: 'Useful child' }] };
    });
    await worker.processBatch(); await assertApplied(f);
    const replacement = (await pool.query('SELECT * FROM memories WHERE vault_id=$1 AND data=$2', [f.vault, memory().statement])).rows[0];
    expect(replacement.source_chunks.sort()).toEqual(chunks.sort());
    expect(replacement.evidence.curation_source_memory_ids.sort()).toEqual(f.ids.slice(0, 2).sort());
    expect((await pool.query('SELECT parent_id FROM memories WHERE id=$1', [f.ids[2]])).rows[0].parent_id).toBe(replacement.id);
    expect((await pool.query('SELECT from_memory_id,to_memory_id,confidence,reason FROM memory_edges WHERE vault_id=$1', [f.vault])).rows)
      .toEqual([{ from_memory_id: replacement.id, to_memory_id: f.ids[2], confidence: 0.7, reason: 'Historic support' }]);
  });

  it.each(['missing', 'foreign'] as const)('rejects %s raw source ownership without partial semantic writes', async kind => {
    const f = await fixture(1);
    const other = kind === 'foreign' ? await fixture(0) : null;
    if (other) await pool.query('DELETE FROM curation_queue WHERE id=$1', [other.queue]);
    const chunk = await source(f.vault);
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1', [f.ids[0], [chunk]]); await syncQueue(f);
    let before!: Awaited<ReturnType<typeof semanticState>>;
    model(async targets => {
      if (other) await db.withTransaction(async client => {
        await client.query('DELETE FROM raw_chunks WHERE id=$1', [chunk]);
        await client.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes) VALUES($1::uuid,$2,'session1','user','gcs',$1::text,20)", [chunk, other.vault]);
      });
      else await pool.query('DELETE FROM raw_chunks WHERE id=$1', [chunk]);
      before = await semanticState(f.vault);
      return { ...empty(), update: [update(targets[0].id)] };
    });
    await worker.processBatch(); await assertRejected(f, before);
  });

  it('retains historic edges to an archived node while applying unrelated unique refinement', async () => {
    const f = await fixture(1), archived = await addMemory(f.vault, 'Historic fact');
    await pool.query("UPDATE memories SET status='superseded',archived_at=now() WHERE id=$1", [archived]);
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'supports',0.8,'Historic edge')", [f.vault, f.ids[0], archived]);
    const beforeEdges = (await semanticState(f.vault)).edges;
    model(targets => ({ ...empty(), update: [update(targets[0].id)] }));
    await worker.processBatch(); await assertApplied(f);
    expect((await semanticState(f.vault)).edges).toEqual(beforeEdges);
  });

  it('fails closed when a same-metadata bucket exceeds the bounded final-state scan', async () => {
    const f = await fixture(1);
    await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,scope,scope_key,status,type,confidence)
      SELECT $1,'Unrelated bounded fact '||n,'Unrelated subject '||n,gen_random_uuid()::text,'session','session1','active','system_fact',0.9
      FROM generate_series(1,501) n`, [f.vault]);
    const before = await semanticState(f.vault);
    const call = model((targets, context) => { expect(context).toEqual([]); return { ...empty(), update: [update(targets[0].id)] }; });
    // Structural overflow is discovered after paid work. It must consume the
    // existing retry budget, not purchase unchanged plans forever as deferrals.
    await pool.query('UPDATE curation_queue SET retry_count=$2 WHERE id=$1', [f.queue, Number(process.env.MAX_CURATION_RETRIES ?? 5) - 1]);
    await worker.processBatch(); await assertRejected(f, before);
    expect((await pool.query('SELECT retry_count FROM curation_queue WHERE id=$1', [f.queue])).rows[0].retry_count)
      .toBe(Number(process.env.MAX_CURATION_RETRIES ?? 5));
    await pool.query("UPDATE curation_queue SET available_at=now()-interval '1 second' WHERE id=$1", [f.queue]);
    await pool.query("UPDATE vault_curation_state SET next_curator_run_at=now()-interval '1 second' WHERE vault_id=$1", [f.vault]);
    await worker.processBatch();
    expect(call).toHaveBeenCalledOnce(); expect(state.embed).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT id FROM curation_queue WHERE id=$1', [f.queue])).rows).toEqual([]);
    expect((await pool.query('SELECT retry_count FROM curation_dead_letter WHERE vault_id=$1', [f.vault])).rows)
      .toEqual([{ retry_count: Number(process.env.MAX_CURATION_RETRIES ?? 5) }]);
    expect(await semanticState(f.vault)).toEqual(before);
    expect((await pool.query('SELECT curator_requests,curator_candidates_processed FROM vault_usage WHERE vault_id=$1', [f.vault])).rows)
      .toEqual([{ curator_requests: 1, curator_candidates_processed: 0 }]);
  });

  it('rejects a survivor with an already archived parent instead of treating the parent as absent', async () => {
    const f = await fixture(1), parent = await addMemory(f.vault, 'Archived parent');
    await pool.query('UPDATE memories SET parent_id=$2 WHERE id=$1', [f.ids[0], parent]);
    await pool.query("UPDATE memories SET status='superseded',archived_at=now() WHERE id=$1", [parent]);
    await syncQueue(f);
    const before = await semanticState(f.vault);
    model((targets, context) => { expect(context).toEqual([]); return { ...empty(), update: [update(targets[0].id)] }; });
    await worker.processBatch(); await assertRejected(f, before);
  });

  it('rejects a cycle completed through an existing hierarchy edge after applying unique updates', async () => {
    const f = await fixture(2);
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'part_of',0.8,'Existing hierarchy')", [f.vault, f.ids[1], f.ids[0]]);
    const before = await semanticState(f.vault);
    model(targets => {
      const sorted = [...targets].sort((a, b) => a.statement.localeCompare(b.statement));
      return { ...empty(), update: [update(sorted[0].id)], keep: [{ id: sorted[1].id, reason: 'Keep parent' }],
        edges: [{ from: sorted[0].id, to: sorted[1].id, type: 'part_of', confidence: 0.8, reason: 'Proposed hierarchy' }] };
    });
    await worker.processBatch(); await assertRejected(f, before);
  });

  it('rolls back a duplicate replacement together with source retirement, child rewiring and evidence edges', async () => {
    const f = await fixture(4), chunks = [await source(f.vault), await source(f.vault)];
    for (let i = 0; i < 2; i++) await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1', [f.ids[i], [chunks[i]]]);
    await pool.query('UPDATE memories SET parent_id=$2 WHERE id=$1', [f.ids[2], f.ids[0]]);
    await pool.query('UPDATE memories SET data=$2 WHERE id=$1', [f.ids[3], memory().statement]);
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason) VALUES($1,$2,$3,'supports',0.7,'Keep original evidence')", [f.vault, f.ids[0], f.ids[2]]);
    await syncQueue(f);
    const before = await semanticState(f.vault);
    model(targets => {
      const sources = targets.filter(t => ['Original fact 0', 'Original fact 1'].includes(t.statement));
      return { ...empty(), consolidate: [consolidate('N1', sources.map(t => t.id))],
        keep: targets.filter(t => !sources.includes(t)).map(t => ({ id: t.id, reason: 'Keep useful fact' })) };
    });
    await worker.processBatch(); await assertRejected(f, before);
  });

  async function scopeSource(f: Fixture, memoryId: string, session = 'session2') {
    const storage = (await import('../services/raw-chunk-storage')).getRawChunkStorage();
    const id = crypto.randomUUID(), content = `This durable fact applies to ${session}.`;
    const ref = await storage.put(id, content);
    await pool.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes)
      VALUES($1,$2,$3,'user',$4,$5,$6)`, [id, f.vault, session, ref.blobStore, ref.blobKey, Buffer.byteLength(content)]);
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1', [memoryId, [id]]);
    await syncQueue(f);
  }

  it.each(['keep', 'consolidate'] as const)('rejects a duplicate introduced only by the final %s scope change', async kind => {
    const f = await fixture(kind === 'keep' ? 1 : 2);
    await scopeSource(f, f.ids[0]);
    const other = await addMemory(f.vault, kind === 'keep' ? 'Original fact 0' : memory().statement);
    await pool.query("UPDATE memories SET scope_key='session2' WHERE id=$1", [other]);
    const before = await semanticState(f.vault);
    const call = model((targets, context, sources) => {
      expect(context).toEqual([]); expect(sources).toHaveLength(1);
      expect(sources[0].content).toContain('session2');
      const ids = targets.map(t => t.id);
      return { ...empty(), keep: kind === 'keep' ? [{ id: ids[0], reason: 'Keep durable fact' }] : [],
        consolidate: kind === 'consolidate' ? [consolidate('N1', ids)] : [],
        scope_changes: [{ id: kind === 'keep' ? ids[0] : 'N1', scope: 'session', scope_key: 'session2', source_refs: ['S1'], reason: 'Source-supported session change' }] };
    });
    await worker.processBatch(); expect(call).toHaveBeenCalledOnce(); await assertRejected(f, before);
    const audit = (await pool.query('SELECT validation_errors FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(JSON.stringify(audit.validation_errors)).toMatch(/duplicate/i);
  });

  it('permits an intermediate update collision when a later source-supported scope change moves the other target away', async () => {
    const f = await fixture(2); await scopeSource(f, f.ids[1]);
    model((targets, _context, sources) => {
      expect(sources).toHaveLength(1);
      const sorted = [...targets].sort((a, b) => a.statement.localeCompare(b.statement));
      return { ...empty(), update: [update(sorted[0].id, memory(sorted[1].statement))], keep: [{ id: sorted[1].id, reason: 'Still useful in another session' }],
        scope_changes: [{ id: sorted[1].id, scope: 'session', scope_key: 'session2', source_refs: ['S1'], reason: 'Source supports separate binding' }] };
    });
    await worker.processBatch(); await assertApplied(f);
    expect((await pool.query('SELECT data,scope_key FROM memories WHERE vault_id=$1 ORDER BY scope_key', [f.vault])).rows)
      .toEqual([{ data: 'Original fact 1', scope_key: 'session1' }, { data: 'Original fact 1', scope_key: 'session2' }]);
    expect((await pool.query('SELECT old_scope_key,new_scope_key FROM memory_scope_change_log WHERE vault_id=$1', [f.vault])).rows)
      .toEqual([{ old_scope_key: 'session1', new_scope_key: 'session2' }]);
  });

  it.each([
    { valid_from: '2019-01-01', valid_until: '2020-12-31' },
    { valid_from: '2099-01-01', valid_until: '2099-12-31' },
  ])('rejects exact ordinary-knowledge duplicates with date window %j', async window => {
    const f = await fixture(2);
    await pool.query('UPDATE memories SET valid_from=$2::date,valid_until=$3::date WHERE vault_id=$1', [f.vault, window.valid_from, window.valid_until]);
    await syncQueue(f);
    const before = await semanticState(f.vault);
    model(targets => ({ ...empty(), update: targets.map(t => update(t.id, { ...memory(), ...window })) }));
    await worker.processBatch(); expect(state.embed).toHaveBeenCalledTimes(2); await assertRejected(f, before);
  });

  it('rejects a survivor with an unseen live parent in another binding', async () => {
    const f = await fixture(1), parent = await addMemory(f.vault, 'Separate session parent');
    await pool.query("UPDATE memories SET scope_key='session2' WHERE id=$1", [parent]);
    await pool.query('UPDATE memories SET parent_id=$2 WHERE id=$1', [f.ids[0], parent]);
    await syncQueue(f);
    const before = await semanticState(f.vault);
    model((targets, context) => { expect(context).toEqual([]); return { ...empty(), update: [update(targets[0].id)] }; });
    await worker.processBatch(); await assertRejected(f, before);
  });

  it('rejects a new relationship to a reviewed node archived by the same plan', async () => {
    const f = await fixture(2), before = await semanticState(f.vault);
    const call = model(targets => ({ ...empty(), keep: [{ id: targets[0].id, reason: 'Keep live fact' }],
      archive: [{ id: targets[1].id, basis: 'explicitly_retracted', reason: 'Retracted fact' }],
      edges: [{ from: targets[0].id, to: targets[1].id, type: 'supports', confidence: 0.8, reason: 'Must not create historical evidence' }] }));
    await worker.processBatch(); expect(call).toHaveBeenCalledOnce(); expect(state.embed).not.toHaveBeenCalled();
    expect(await semanticState(f.vault)).toEqual(before);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows).toEqual([{ validation_status: 'invalid' }]);
    expect((await pool.query("SELECT action_key FROM worker_action_receipts WHERE queue_id=$1 AND action_key LIKE 'apply-actions:%'", [f.queue])).rows).toEqual([]);
    expect((await pool.query('SELECT curator_candidates_processed FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0].curator_candidates_processed).toBe(0);
  });

  it('applies a unique replacement exactly once with no repeated embeddings, mutations or accounting on another batch', async () => {
    const f = await fixture(2);
    const call = model(targets => ({ ...empty(), consolidate: [consolidate('N1', targets.map(t => t.id))] }));
    await worker.processBatch(); await assertApplied(f);
    const before = await semanticState(f.vault);
    const usage = (await pool.query('SELECT * FROM vault_usage WHERE vault_id=$1', [f.vault])).rows;
    const receipts = (await pool.query('SELECT * FROM worker_action_receipts WHERE queue_id=$1 ORDER BY action_key', [f.queue])).rows;
    await worker.processBatch();
    expect(call).toHaveBeenCalledOnce(); expect(state.embed).toHaveBeenCalledOnce();
    expect(await semanticState(f.vault)).toEqual(before);
    expect((await pool.query('SELECT * FROM vault_usage WHERE vault_id=$1', [f.vault])).rows).toEqual(usage);
    expect((await pool.query('SELECT * FROM worker_action_receipts WHERE queue_id=$1 ORDER BY action_key', [f.queue])).rows).toEqual(receipts);
  });

  it('permits a consolidation identical to one of its own sources retired later in the transaction', async () => {
    const f = await fixture(2);
    model(targets => ({ ...empty(), consolidate: [consolidate('N1', targets.map(t => t.id), memory('Original fact 0'))] }));
    await worker.processBatch(); await assertApplied(f);
    expect((await pool.query("SELECT data FROM memories WHERE vault_id=$1 AND status='active' AND archived_at IS NULL", [f.vault])).rows)
      .toEqual([{ data: 'Original fact 0' }]);
    expect((await pool.query("SELECT id FROM memories WHERE vault_id=$1 AND status='superseded'", [f.vault])).rows.map(r => r.id).sort()).toEqual([...f.ids].sort());
  });

  it('permits a consolidation collision resolved by a later update of the other target', async () => {
    const f = await fixture(3);
    model(targets => {
      const sorted = [...targets].sort((a, b) => a.statement.localeCompare(b.statement));
      return { ...empty(), consolidate: [consolidate('N1', sorted.slice(0, 2).map(t => t.id), memory(sorted[2].statement))],
        update: [update(sorted[2].id, memory('Fresh terminal fact'))] };
    });
    await worker.processBatch(); await assertApplied(f);
    expect((await pool.query("SELECT data FROM memories WHERE vault_id=$1 AND status='active' AND archived_at IS NULL ORDER BY data", [f.vault])).rows)
      .toEqual([{ data: 'Fresh terminal fact' }, { data: 'Original fact 2' }]);
  });

  it.each([false, true])('permits final-unique consolidation swaps with reversed action order=%s', async reverse => {
    const f = await fixture(4);
    model(targets => {
      const sorted = [...targets].sort((a, b) => a.statement.localeCompare(b.statement));
      const actions = [consolidate('N1', sorted.slice(0, 2).map(t => t.id), memory(sorted[2].statement)),
        consolidate('N2', sorted.slice(2).map(t => t.id), memory(sorted[0].statement))];
      return { ...empty(), consolidate: reverse ? actions.reverse() : actions };
    });
    await worker.processBatch(); await assertApplied(f);
    expect((await pool.query("SELECT data FROM memories WHERE vault_id=$1 AND status='active' AND archived_at IS NULL ORDER BY data", [f.vault])).rows)
      .toEqual([{ data: 'Original fact 0' }, { data: 'Original fact 2' }]);
    expect((await pool.query("SELECT id FROM memories WHERE vault_id=$1 AND status='superseded'", [f.vault])).rows).toHaveLength(4);
  });
});
