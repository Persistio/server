import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const providers = vi.hoisted(() => ({ embed: vi.fn(), read: vi.fn(), metric: vi.fn() }));
vi.mock('node:worker_threads', async original => ({ ...(await original() as typeof import('node:worker_threads')), parentPort: null }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embedBatch: providers.embed }) }));
vi.mock('../services/raw-chunk-storage', () => ({ getRawChunkStorage: () => ({ store: 'local', get: providers.read }) }));
vi.mock('../services/customer-metrics', async original => ({ ...(await original() as typeof import('../services/customer-metrics')), recordCustomerMetric: providers.metric }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('actual extraction worker commit lifecycle (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [], queues: string[] = [];
  let processBatch: typeof import('./extraction-worker').processBatch;
  let db: typeof import('../db/client');
  let config: ReturnType<typeof import('../config').getConfig>;
  let extractor: typeof import('../services/extractor').ExtractorService;
  let facts: ReturnType<typeof vi.spyOn>, summary: ReturnType<typeof vi.spyOn>, aliases: ReturnType<typeof vi.spyOn>;
  let vector: number[];
  const candidate = (fact = 'The project uses PostgreSQL for durable storage.') => ({ fact, subject: 'Project storage', score: 9,
    salience: 0.9, sensitivity: 'low', type: 'system_fact', scope: 'project', polarity: 'neutral', volatility: 'low',
    evidence: 'User statement', source_refs:['S1'],scope_basis:'Explicit project context',valid_from: null, valid_until: null });
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    db = await import('../db/client'); await db.runMigrations();
    config = (await import('../config')).getConfig();
    config.ENCRYPTION_ENABLED = false; config.CURATOR_AUTO_RUN = false;
    config.EXTRACTION_BATCH_SIZE = 10; config.EXTRACTION_WORKER_CONCURRENCY = 2;
    vector = [1, ...Array(config.STORAGE_EMBEDDING_DIMENSIONS - 1).fill(0)];
    extractor = (await import('../services/extractor')).ExtractorService;
    ({ processBatch } = await import('./extraction-worker'));
  });
  beforeEach(() => {
    facts = vi.spyOn(extractor.prototype, 'extractFacts').mockImplementation(async conversation => {
      const parsed=JSON.parse(conversation);
      return [{...candidate(),source_refs:[parsed.sources.find((s:any)=>s.current).ref]}] as never;
    });
    summary = vi.spyOn(extractor.prototype, 'extractSessionContext').mockResolvedValue('Committed session summary');
    aliases = vi.spyOn(extractor.prototype, 'extractSessionAliases').mockResolvedValue([{ alias: 'DB', canonical: 'Database' }]);
    providers.embed.mockReset().mockImplementation(async (texts: string[]) => texts.map(() => vector));
    providers.read.mockReset().mockResolvedValue('The project uses PostgreSQL for durable storage.');
    providers.metric.mockReset();
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])', [queues]);
    await pool.query('DELETE FROM extraction_queue WHERE vault_id=ANY($1::uuid[])',[vaults]);
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]);
    vaults.length = 0; queues.length = 0;
  });
  afterAll(async () => { await pool.end(); await db?.closePool(); });

  async function fixture(options: { vault?: string; session?: string; legacy?: boolean } = {}) {
    const vault = options.vault ?? crypto.randomUUID(), account = crypto.randomUUID();
    const chunk = crypto.randomUUID(), segment = crypto.randomUUID(), queue = crypto.randomUUID(), job = crypto.randomUUID();
    const session = options.session ?? `session-${crypto.randomUUID()}`;
    if (!options.vault) {
      vaults.push(vault);
      await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id,account_id) VALUES ($1,'extraction-regression',$2,'unlimited',$3)", [vault, crypto.randomUUID(), account]);
    }
    queues.push(queue);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,capture_context) VALUES ($1,$2,$3,'user','local',$4,100,jsonb_build_object('session_id',$3::text,'project_id','project'))", [chunk, vault, session, `test/${chunk}`]);
    await pool.query("INSERT INTO segments(id,vault_id,session_id,project_id,chunk_ids) VALUES ($1,$2,$3,'project',$4::uuid[])", [segment, vault, session, [chunk]]);
    await pool.query("INSERT INTO jobs(id,vault_id,kind) VALUES ($1,$2,'bulk_ingest')", [job, vault]);
    await pool.query('INSERT INTO extraction_queue(id,vault_id,segment_id,chunk_id,job_id) VALUES ($1,$2,$3,$4,$5)',
      [queue, vault, options.legacy ? null : segment, options.legacy ? chunk : null, job]);
    return { vault, account, chunk, segment, queue, job };
  }
  async function status(f: Awaited<ReturnType<typeof fixture>>) {
    return {
      queue: (await pool.query('SELECT retry_count,claim_token,last_error FROM extraction_queue WHERE id=$1', [f.queue])).rows,
      job: (await pool.query('SELECT status FROM jobs WHERE id=$1', [f.job])).rows[0].status,
      memories: (await pool.query('SELECT data,status FROM memories WHERE vault_id=$1', [f.vault])).rows,
      usage: (await pool.query('SELECT memory_adds FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0]?.memory_adds ?? 0,
      processed: (await pool.query('SELECT processed FROM raw_chunks WHERE id=$1', [f.chunk])).rows[0].processed,
      receipts: (await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount
    };
  }
  const deltas = () => providers.metric.mock.calls.map(([event]) => event).filter(event => event.event_type === 'quota_delta');

  it.each([false, true])('commits the real %s legacy/segment path and both metric dimensions only after commit', async legacy => {
    const f = await fixture({ legacy });
    const observed: Promise<unknown>[] = [];
    providers.metric.mockImplementation(event => {
      if (event.event_type === 'quota_delta') observed.push(status(f).then(s => expect(s).toMatchObject({ job: 'completed', queue: [], usage: 1, processed: true, receipts: 1 })));
    });
    await processBatch(f.vault); await Promise.all(observed);
    expect((await status(f)).memories).toHaveLength(1);
    expect(deltas()).toEqual(expect.arrayContaining([
      expect.objectContaining({ memory_adds_delta: 1, workspace_id: f.account, vault_id: f.vault, source: 'extraction_worker' }),
      expect.objectContaining({ memory_count_delta: 1, workspace_id: f.account, vault_id: f.vault, source: 'extraction_worker' })
    ]));
    expect(deltas()).toHaveLength(2);
    await processBatch(f.vault);
    expect(deltas()).toHaveLength(2);
  });
  it('finishes zero-candidate work without quota/count effects', async () => {
    const f = await fixture(); facts.mockResolvedValue([]);
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ queue: [], job: 'completed', memories: [], usage: 0, processed: true, receipts: 1 });
    expect(deltas()).toEqual([]);
  });
  it.each([false, true])('never promotes ambiguous forwarded content into a human rule, and fences stale work (stale=%s)', async stale => {
    const f = await fixture();
    facts.mockResolvedValue([{ ...candidate(), type: 'user_rule' }] as never);
    providers.read.mockImplementation(async () => {
      if (stale) await pool.query("UPDATE extraction_queue SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [f.queue]);
      return '[Inter-session message] sourceSession=first sourceSession=second isUser=maybe\npayload';
    });
    await processBatch(f.vault);
    if (stale) { expect(summary).not.toHaveBeenCalled(); expect(facts).not.toHaveBeenCalled(); }
    else expect(facts).toHaveBeenCalledOnce();
    expect(await status(f)).toMatchObject(stale
      ? {job:'running',processed:false,receipts:0,memories:[],usage:0,queue:[expect.objectContaining({retry_count:0})]}
      : {job:'completed',processed:true,receipts:1,memories:[],usage:0,queue:[]});
    expect(deltas()).toEqual([]);
  });
  it('rolls back an earlier memory, embedding, receipt and charge when the later candidate fails SQL', async () => {
    const f = await fixture(); facts.mockResolvedValue([candidate(), candidate('The project also uses durable queues.')] as never);
    providers.embed.mockImplementation(async (texts: string[]) => texts.map((text, index) => text.startsWith('The project') && index === 1 ? [1, 2] : vector));
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ job: 'running', memories: [], usage: 0, processed: false, receipts: 0,
      queue: [expect.objectContaining({ retry_count: 1, claim_token: null })] });
    expect(deltas()).toEqual([]);
    expect((await pool.query("SELECT 1 FROM entity_aliases WHERE vault_id=$1 AND scope='project'", [f.vault])).rowCount).toBe(1);
    expect((await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[f.vault])).rows).toEqual([]);
    expect((await pool.query('SELECT id FROM curation_queue WHERE vault_id=$1',[f.vault])).rows).toEqual([]);
    providers.embed.mockImplementation(async (texts:string[])=>texts.map(()=>vector));
    await processBatch(f.vault);
    const recovered=await status(f);
    expect(recovered).toMatchObject({job:'completed',processed:true,receipts:1,queue:[]});
    // Equal synthetic vectors can deduplicate the second fact; paid quota and
    // mutations must describe only this successful transaction, never the rollback.
    expect(recovered.usage).toBe(recovered.memories.length);
    expect(recovered.memories.length).toBeGreaterThan(0);
    const mutations=(await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[f.vault])).rows;
    await processBatch(f.vault);expect(await status(f)).toEqual(recovered);expect(facts).toHaveBeenCalledTimes(2);
    expect((await pool.query('SELECT id FROM memory_mutation_events WHERE vault_id=$1',[f.vault])).rows).toEqual(mutations);
  });
  it('rolls back a prior insertion when a later candidate exhausts the transaction quota', async () => {
    const f = await fixture(); facts.mockResolvedValue([candidate(), candidate('The project also uses durable queues.')] as never);
    await pool.query('UPDATE vaults SET rate_limit_override=$2::jsonb WHERE id=$1', [f.vault, JSON.stringify({ memory_adds_per_month: 1 })]);
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ memories: [], usage: 0, receipts: 0, processed: false,
      queue: [expect.objectContaining({ retry_count: 1, last_error: 'memory_adds quota exceeded' })] });
    expect(deltas()).toEqual([]);
  });
  it('rolls back the winning context if its initial alias SQL fails, and allows a fresh retry', async () => {
    const f = await fixture();
    await pool.query(`CREATE FUNCTION pr370_fail_alias() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.vault_id='${f.vault}'::uuid THEN RAISE EXCEPTION 'initial alias failed'; END IF; RETURN NEW; END $$`);
    await pool.query('CREATE TRIGGER pr370_fail_alias BEFORE INSERT ON entity_aliases FOR EACH ROW EXECUTE FUNCTION pr370_fail_alias()');
    try { await processBatch(f.vault); }
    finally { await pool.query('DROP TRIGGER pr370_fail_alias ON entity_aliases'); await pool.query('DROP FUNCTION pr370_fail_alias()'); }
    expect((await pool.query('SELECT 1 FROM session_contexts WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
    expect(facts).not.toHaveBeenCalled();
    await processBatch(f.vault);
    expect(summary).toHaveBeenCalledTimes(2);
    expect((await status(f)).job).toBe('completed');
  });
  it('does not retry committed work when either publisher throws and still attempts the other dimension', async () => {
    const f = await fixture(); providers.metric.mockImplementation(() => { throw new Error('Publisher unavailable'); });
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ queue: [], job: 'completed', usage: 1, processed: true, receipts: 1 });
    expect(deltas()).toHaveLength(2);
  });
  it.each(['summary', 'aliases', 'embedding'] as const)('fences auxiliary/final writes after losing ownership during %s preparation', async stage => {
    const f = await fixture();
    const lose = async () => { await pool.query("UPDATE extraction_queue SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", [f.queue]); };
    if (stage === 'summary') summary.mockImplementation(async () => { await lose(); return 'Stale summary'; });
    if (stage === 'aliases') aliases.mockImplementation(async () => { await lose(); return [{ alias: 'stale', canonical: 'stale proposal' }]; });
    if (stage === 'embedding') providers.embed.mockImplementation(async (texts: string[]) => { await lose(); return texts.map(() => vector); });
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ job: 'running', memories: [], usage: 0, processed: false, receipts: 0,
      queue: [expect.objectContaining({ retry_count: 0, claim_token: expect.any(String) })] });
    if (stage !== 'embedding') {
      expect((await pool.query('SELECT 1 FROM session_contexts WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
      expect((await pool.query('SELECT 1 FROM entity_aliases WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
    } else expect((await pool.query("SELECT 1 FROM entity_aliases WHERE vault_id=$1 AND scope='project'", [f.vault])).rowCount).toBe(0);
    expect(deltas()).toEqual([]);
  });
  it('lets two same-session jobs establish only one summary with its own initial aliases', async () => {
    const session = 'shared-session'; const first = await fixture({ session }); await fixture({ session, vault: first.vault });
    let ready = 0; let release!: () => void; const both = new Promise<void>(resolve => { release = resolve; });
    summary.mockImplementation(async () => { const index = ++ready; if (ready === 2) release(); await both; return `Summary ${index}`; });
    let aliasIndex = 0;
    aliases.mockImplementation(async () => [{ alias: `alias${++aliasIndex}`, canonical: 'shared canonical' }]);
    await processBatch(first.vault);
    expect(summary).toHaveBeenCalledTimes(2);
    const contexts = (await pool.query('SELECT context FROM session_contexts WHERE vault_id=$1', [first.vault])).rows;
    const initialAliases = (await pool.query("SELECT alias FROM entity_aliases WHERE vault_id=$1 AND scope='session'", [first.vault])).rows;
    expect(contexts).toHaveLength(1); expect(initialAliases).toHaveLength(1);
    expect(initialAliases[0].alias).toBe(`alias${contexts[0].context.slice(-1)}`);
    expect((await status(first)).job).toBe('completed');
  });
  it('dead-letters exhausted work under the same fence and marks the persistent job failed', async () => {
    const f = await fixture();
    await pool.query('UPDATE extraction_queue SET retry_count=$2 WHERE id=$1', [f.queue, config.MAX_EXTRACTION_RETRIES - 1]);
    facts.mockRejectedValue(new Error('Invalid provider output'));
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ queue: [], job: 'failed', memories: [], usage: 0, receipts: 1 });
    expect((await pool.query('SELECT 1 FROM extraction_dead_letter WHERE job_id=$1', [f.job])).rowCount).toBe(1);
    expect(deltas()).toEqual([]);
  });
  it.each(['budget', 'circuit'] as const)('defers real extraction on %s without retry increments or committed effects', async reason => {
    const f = await fixture();
    const { AiBudgetDeferredError } = await import('../services/usage');
    const { CircuitBreakerOpenError } = await import('../services/ai-resilience');
    facts.mockRejectedValue(reason === 'budget' ? new AiBudgetDeferredError('extraction', new Date(Date.now()+60_000), 60_000) : new CircuitBreakerOpenError('extractor', 60_000));
    await processBatch(f.vault);
    expect(await status(f)).toMatchObject({ job: 'running', processed: false, memories: [], usage: 0, receipts: 0,
      queue: [expect.objectContaining({ retry_count: 0, claim_token: null })] });
    expect(deltas()).toEqual([]);
  });
});
