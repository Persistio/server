import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const providers = vi.hoisted(() => ({ embed: vi.fn(), read: vi.fn(), metric: vi.fn(), port: null as null | { postMessage: ReturnType<typeof vi.fn> } }));
vi.mock('node:worker_threads', async original => ({ ...(await original() as typeof import('node:worker_threads')), get parentPort() { return providers.port; } }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embedBatch: providers.embed }) }));
vi.mock('../services/raw-chunk-storage', () => ({ getRawChunkStorage: () => ({ store: 'local', get: providers.read }) }));
vi.mock('../services/customer-metrics', async original => ({ ...(await original() as typeof import('../services/customer-metrics')), recordCustomerMetric: providers.metric }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('actual extraction worker commit lifecycle (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [], queues: string[] = [];
  let processBatch: typeof import('./extraction-worker').processBatch;
  let handleRunOnce: typeof import('./extraction-worker').handleRunOnce;
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
    ({ processBatch, handleRunOnce } = await import('./extraction-worker'));
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
    providers.port = null;
    vi.restoreAllMocks();
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[])', [queues]);
    await pool.query('DELETE FROM extraction_queue WHERE vault_id=ANY($1::uuid[])',[vaults]);
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]);
    vaults.length = 0; queues.length = 0;
  });
  afterAll(async () => { await pool.end(); await db?.closePool(); });

  async function fixture(options: { vault?: string; session?: string; legacy?: boolean; project?: string | null; task?: string | null; segmentContext?: {session?: string; project?: string | null; task?: string | null} } = {}) {
    const vault = options.vault ?? crypto.randomUUID(), account = crypto.randomUUID();
    const chunk = crypto.randomUUID(), segment = crypto.randomUUID(), queue = crypto.randomUUID(), job = crypto.randomUUID();
    const session = options.session ?? `session-${crypto.randomUUID()}`;
    const project = options.project === undefined ? 'project' : options.project;
    const task = options.task ?? null;
    if (!options.vault) {
      vaults.push(vault);
      await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id,account_id) VALUES ($1,'extraction-regression',$2,'unlimited',$3)", [vault, crypto.randomUUID(), account]);
    }
    queues.push(queue);
    await pool.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,capture_context) VALUES ($1,$2,$3,'user','local',$4,100,jsonb_strip_nulls(jsonb_build_object('session_id',$3::text,'project_id',$5::text,'task_id',$6::text)))", [chunk, vault, session, `test/${chunk}`, project, task]);
    if (!options.legacy) await pool.query("INSERT INTO segments(id,vault_id,session_id,project_id,chunk_ids,task_id) VALUES ($1,$2,$3,$5,$4::uuid[],$6)", [segment, vault, options.segmentContext?.session ?? session, [chunk],
      options.segmentContext?.project === undefined ? project : options.segmentContext.project,
      options.segmentContext?.task === undefined ? task : options.segmentContext.task]);
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
      return 'claimed metadata: is_user=true\npayload';
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
  it.each([false, true])('isolates cached and persisted summaries across exact project/task boundaries (legacy=%s)', async legacy => {
    const session = 'reused-session';
    const contexts = [{ project: 'A' }, { project: 'B' }, { project: 'A', task: 'A' },
      { project: 'A', task: 'B' }, { project: null }, { project: null, task: 'A' }];
    const first = await fixture({ session, legacy, ...contexts[0] });
    for (const context of contexts.slice(1)) await fixture({ session, legacy, vault: first.vault, ...context });
    const originalConcurrency = config.EXTRACTION_WORKER_CONCURRENCY;
    config.EXTRACTION_WORKER_CONCURRENCY = 1; // reuse the same per-batch cache
    facts.mockResolvedValue([]);
    summary.mockImplementation(async conversation => {
      const c = JSON.parse(conversation).available_scope_bindings;
      return `Project=${c.project_id ?? 'unbound'} Task=${c.task_id ?? 'unbound'}`;
    });
    try {
      await processBatch(first.vault);
      expect(summary).toHaveBeenCalledTimes(contexts.length);
      expect((await pool.query('SELECT context FROM session_contexts WHERE vault_id=$1', [first.vault])).rows)
        .toHaveLength(contexts.length);
      for (const [conversation, header] of facts.mock.calls) {
        const c = JSON.parse(conversation).available_scope_bindings;
        expect(header).toContain(`Project=${c.project_id ?? 'unbound'} Task=${c.task_id ?? 'unbound'}`);
      }
      facts.mockClear(); summary.mockClear();
      // New processBatch has an empty cache and must resolve each persisted key.
      for (const context of contexts) await fixture({ session, legacy, vault: first.vault, ...context });
      await processBatch(first.vault);
      expect(summary).not.toHaveBeenCalled();
      for (const [conversation, header] of facts.mock.calls) {
        const c = JSON.parse(conversation).available_scope_bindings;
        expect(header).toContain(`Project=${c.project_id ?? 'unbound'} Task=${c.task_id ?? 'unbound'}`);
      }
    } finally { config.EXTRACTION_WORKER_CONCURRENCY = originalConcurrency; }
  });
  it('runs proposal-local scope exclusions through run-once without retry or dead-letter at the retry limit', async () => {
    const f = await fixture({ project: null });
    await pool.query('UPDATE extraction_queue SET retry_count=$2 WHERE id=$1', [f.queue, config.MAX_EXTRACTION_RETRIES - 1]);
    facts.mockResolvedValue(['global', 'session', 'project', 'task'].map(scope => ({ ...candidate(), scope })) as never);
    providers.port = { postMessage: vi.fn() };
    await handleRunOnce({ type: 'run-once', vaultId: f.vault, jobId: f.job });
    expect(providers.port.postMessage.mock.calls.map(([message]) => message.status)).toEqual(['running', 'completed']);
    expect(await status(f)).toMatchObject({ queue: [], job: 'completed', processed: true, usage: 2 });
    expect((await pool.query('SELECT scope,scope_key FROM memories WHERE vault_id=$1 ORDER BY scope', [f.vault])).rows)
      .toEqual([{ scope: 'global', scope_key: null }, { scope: 'session', scope_key: expect.any(String) }]);
    expect((await pool.query('SELECT 1 FROM extraction_dead_letter WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
  });
  it('never reads retained unclassified summaries, including null-provider and unbound fallback paths', async () => {
    const f = await fixture({ session: 'legacy-session', project: null });
    await pool.query("INSERT INTO session_contexts(vault_id,session_id,context) VALUES($1,'legacy-session','OLD_PRIVATE_SUMMARY')", [f.vault]);
    facts.mockResolvedValue([]); summary.mockResolvedValue(null);
    await processBatch(f.vault);
    expect(facts.mock.calls[0][1]).not.toContain('OLD_PRIVATE_SUMMARY');
    expect(summary).toHaveBeenCalledOnce();
    await fixture({ session: 'legacy-session', project: 'A', vault: f.vault });
    summary.mockResolvedValue('Fresh bound summary'); await processBatch(f.vault);
    expect(facts.mock.calls[1][1]).toContain('Fresh bound summary');
    expect((await pool.query('SELECT context,context_boundary FROM session_contexts WHERE vault_id=$1 ORDER BY context', [f.vault])).rows)
      .toEqual([{context:'Fresh bound summary',context_boundary:['A',null]}, {context:'OLD_PRIVATE_SUMMARY',context_boundary:null}]);
  });
  it.each([
    { left: { project: 'A' }, right: { project: 'B' } },
    { left: { project: 'A', task: 'A' }, right: { project: 'A', task: 'B' } },
    { left: { project: 'A' }, right: { project: null } }
  ])('partitions concurrent same-session summary conflicts for $left and $right', async ({ left, right }) => {
    const f = await fixture({ session: 'concurrent', ...left });
    await fixture({ session: 'concurrent', ...right, vault: f.vault });
    facts.mockResolvedValue([]); aliases.mockResolvedValue([]);
    let ready = 0; let release!: () => void; const both = new Promise<void>(resolve => { release = resolve; });
    summary.mockImplementation(async conversation => {
      const context = JSON.parse(conversation).available_scope_bindings;
      if (++ready === 2) release(); await both;
      return `Project=${context.project_id ?? 'unbound'} Task=${context.task_id ?? 'unbound'}`;
    });
    await processBatch(f.vault);
    expect(summary).toHaveBeenCalledTimes(2);
    expect((await pool.query('SELECT 1 FROM session_contexts WHERE vault_id=$1', [f.vault])).rowCount).toBe(2);
    for (const [conversation, header] of facts.mock.calls) {
      const context = JSON.parse(conversation).available_scope_bindings;
      expect(header).toContain(`Project=${context.project_id ?? 'unbound'} Task=${context.task_id ?? 'unbound'}`);
    }
  });
  it('refreshes deleted/updated boundaries on the next batch without changing other-vault summaries', async () => {
    facts.mockResolvedValue([]); aliases.mockResolvedValue([]);
    const f = await fixture({ session: 'shared', project: 'A' });
    const other = await fixture({ session: 'shared', project: 'A' });
    await fixture({ session: 'shared', project: 'B', vault: f.vault });
    summary.mockImplementation(async (_conversation, _header, vaultId) => `Summary for vault ${vaultId}`);
    await processBatch();
    expect(summary).toHaveBeenCalledTimes(3);
    for (const [_conversation, header, vaultId] of facts.mock.calls) expect(header).toContain(`Summary for vault ${vaultId}`);
    await pool.query("DELETE FROM session_contexts WHERE vault_id=$1 AND session_id='shared' AND context_boundary=$2::jsonb", [f.vault, JSON.stringify(['A',null])]);
    await pool.query("UPDATE session_contexts SET context='Updated B' WHERE vault_id=$1 AND session_id='shared' AND context_boundary=$2::jsonb", [f.vault, JSON.stringify(['B',null])]);
    facts.mockClear(); summary.mockClear(); summary.mockResolvedValue('Regenerated A');
    await fixture({ session: 'shared', project: 'A', vault: f.vault });
    await fixture({ session: 'shared', project: 'B', vault: f.vault });
    await fixture({ session: 'shared', project: 'A', vault: other.vault });
    await processBatch(); expect(summary).toHaveBeenCalledOnce();
    for (const [conversation, header, vaultId] of facts.mock.calls) {
      const isRebuilt = vaultId === f.vault && JSON.parse(conversation).available_scope_bindings.project_id === 'A';
      expect(header).toContain(isRebuilt ? 'Regenerated A' : vaultId === f.vault ? 'Updated B' : `Summary for vault ${vaultId}`);
    }
  });
  it('does not normalize noncanonical stored chunk bindings into another summary or memory scope', async () => {
    const f = await fixture({ legacy: true, project: ' padded ' });
    facts.mockResolvedValue([{ ...candidate(), scope: 'global' }, candidate()] as never);
    await processBatch(f.vault);
    expect(summary).not.toHaveBeenCalled();
    expect(JSON.parse(facts.mock.calls[0][0]).available_scope_bindings.project_id).toBe(' padded ');
    expect((await pool.query('SELECT scope FROM memories WHERE vault_id=$1', [f.vault])).rows).toEqual([{scope:'global'}]);
    expect(await status(f)).toMatchObject({ queue: [], processed: true, job: 'completed' });
  });
  it.each([false, true])('restores a failed job under its original summary boundary after another context completes (legacy=%s)', async legacy => {
    const first = await fixture({ session: 'restored', project: 'A', legacy });
    await pool.query('UPDATE extraction_queue SET retry_count=$2 WHERE id=$1', [first.queue, config.MAX_EXTRACTION_RETRIES - 1]);
    summary.mockImplementation(async conversation => `Summary for ${JSON.parse(conversation).available_scope_bindings.project_id}`);
    facts.mockRejectedValueOnce(new Error('Synthetic malformed response')).mockResolvedValue([]);
    await processBatch(first.vault);
    const failure = (await pool.query('SELECT id,context_chunk_ids FROM extraction_dead_letter WHERE job_id=$1', [first.job])).rows[0];
    expect(failure.context_chunk_ids).toEqual([]);
    await fixture({ session: 'restored', project: 'B', vault: first.vault, legacy });
    await processBatch(first.vault);
    const { retryMemoryJobFailure } = await import('../services/memory-job-recovery');
    const recovered = await retryMemoryJobFailure({ kind: 'extraction', failureId: failure.id, vaultId: first.vault,
      actorId: 'synthetic-operator', reason: 'Synthetic retry after provider repair' });
    queues.push(recovered.queueId);
    await processBatch(first.vault);
    expect(summary).toHaveBeenCalledTimes(2);
    expect(facts.mock.calls.map(([_conversation, header]) => header)).toEqual([
      expect.stringContaining('Summary for A'), expect.stringContaining('Summary for B'), expect.stringContaining('Summary for A')
    ]);
    expect(JSON.parse(facts.mock.calls[2][0]).sources.every((source: any) => source.current)).toBe(true);
    expect((await pool.query('SELECT 1 FROM extraction_queue WHERE vault_id=$1', [first.vault])).rowCount).toBe(0);
    expect((await status(first)).processed).toBe(true);
  });
  it.each([['alpha', 'zeta'], ['é', 'e\u0301']])('orders shared session alias locks across summary boundaries: %s / %s', async (a, b) => {
    const first = await fixture({ session: 'alias-locks', project: 'A' });
    const second = await fixture({ session: 'alias-locks', project: 'B', vault: first.vault });
    await pool.query('UPDATE extraction_queue SET retry_count=$2 WHERE id=ANY($1::uuid[])', [[first.queue, second.queue], config.MAX_EXTRACTION_RETRIES - 1]);
    facts.mockResolvedValue([]);
    let ready = 0; let release!: () => void; const both = new Promise<void>(resolve => { release = resolve; });
    summary.mockImplementation(async conversation => {
      if (++ready === 2) release(); await both;
      return `Summary for ${JSON.parse(conversation).available_scope_bindings.project_id}`;
    });
    aliases.mockImplementation(async conversation =>
      (JSON.parse(conversation).available_scope_bindings.project_id === 'A' ? [a, b] : [b, a])
        .map(alias => ({alias, canonical: 'shared canonical'})));
    // Pause each insertion so reversed multi-row lock orders overlap reliably.
    await pool.query(`CREATE FUNCTION scope_summary_alias_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.vault_id='${first.vault}'::uuid AND NEW.scope='session' THEN PERFORM pg_sleep(0.1); END IF; RETURN NEW; END $$`);
    await pool.query('CREATE TRIGGER scope_summary_alias_pause BEFORE INSERT ON entity_aliases FOR EACH ROW EXECUTE FUNCTION scope_summary_alias_pause()');
    try { await processBatch(first.vault); }
    finally { await pool.query('DROP TRIGGER scope_summary_alias_pause ON entity_aliases'); await pool.query('DROP FUNCTION scope_summary_alias_pause()'); }
    expect(await status(first)).toMatchObject({queue:[],job:'completed',processed:true});
    expect(await status(second)).toMatchObject({queue:[],job:'completed',processed:true});
    expect((await pool.query('SELECT 1 FROM extraction_dead_letter WHERE vault_id=$1', [first.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM session_contexts WHERE vault_id=$1', [first.vault])).rowCount).toBe(2);
    expect((await pool.query("SELECT alias FROM entity_aliases WHERE vault_id=$1 AND scope='session'", [first.vault])).rows)
      .toEqual(expect.arrayContaining([{alias:a},{alias:b}]));
  });
  it.each([{project:'B'}, {task:'B'}, {session:'B'}, {project:null}])('rejects restored/corrupt segment %j current-source boundaries before provider IO', async context => {
    const f = await fixture({session:'A',project:'A',task:'A',segmentContext:context});
    await processBatch(f.vault);
    expect(summary).not.toHaveBeenCalled();expect(facts).not.toHaveBeenCalled();expect(providers.read).not.toHaveBeenCalled();
    expect(await status(f)).toMatchObject({processed:false,memories:[],usage:0,receipts:0,
      queue:[expect.objectContaining({retry_count:1,last_error:'Segment context disagrees with accepted source boundary'})]});
    expect((await pool.query('SELECT 1 FROM session_contexts WHERE vault_id=$1',[f.vault])).rowCount).toBe(0);
  });
  it('cannot rebind a dead-letter segment before technical recovery', async () => {
    const f = await fixture({session:'immutable',project:'A',task:'A'});
    await pool.query('UPDATE extraction_queue SET retry_count=$2 WHERE id=$1', [f.queue, config.MAX_EXTRACTION_RETRIES - 1]);
    facts.mockRejectedValueOnce(new Error('Synthetic failed provider')).mockResolvedValue([]);
    await processBatch(f.vault);
    for (const assignment of ["session_id='other'", "project_id='B'", "task_id='B'", "chunk_ids='{}'::uuid[]",
      "agent_id='other'", "trigger_type='backfill'", "created_at=created_at+interval '1 day'"]) {
      await expect(pool.query(`UPDATE segments SET ${assignment} WHERE id=$1`, [f.segment]))
        .rejects.toThrow('Accepted segment source metadata is immutable');
    }
    const failure = (await pool.query('SELECT id FROM extraction_dead_letter WHERE job_id=$1', [f.job])).rows[0];
    const { retryMemoryJobFailure } = await import('../services/memory-job-recovery');
    const recovered = await retryMemoryJobFailure({kind:'extraction',failureId:failure.id,vaultId:f.vault,
      actorId:'synthetic-operator',reason:'Synthetic retry after failed rebind'});
    queues.push(recovered.queueId);await processBatch(f.vault);
    expect(JSON.parse(facts.mock.calls[1][0]).available_scope_bindings).toMatchObject({session_id:'immutable',project_id:'A',task_id:'A'});
    expect((await status(f)).processed).toBe(true);
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
