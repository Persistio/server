import crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import Fastify from 'fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({ pool: null as any, port: null as any, vault: null as any,
  blobs: new Map<string, string>(), dimensions: 1536, extract: vi.fn(), context: vi.fn(async () => null) }));
vi.mock('../../db/client', () => ({
  query: (sql: string, args: unknown[] = []) => sql.includes('WITH claimed AS') && args.length === 2
    ? Promise.resolve({ rows: [], rowCount: 0 }) : state.pool.query(sql, args),
  withTransaction: async (callback: (client: any) => unknown) => {
    const client = await state.pool.connect();
    try { await client.query('BEGIN'); const result = await callback(client); await client.query('COMMIT'); return result; }
    catch (error) { await client.query('ROLLBACK'); throw error; } finally { client.release(); }
  },
  closePool: async () => {}
}));
vi.mock('node:worker_threads', async original => ({ ...await original<typeof import('node:worker_threads')>(),
  get parentPort() { return state.port; }
}));
vi.mock('../../config', async original => {
  const actual = await original<typeof import('../../config')>();
  return { ...actual, getConfig: () => ({ ...actual.getConfig(), ENCRYPTION_ENABLED: false, CURATOR_AUTO_RUN: false,
    EXTRACTION_INTERVAL_MS: 600_000, EMBEDDING_DIMENSIONS: state.dimensions }) };
});
vi.mock('../../middleware/auth', () => ({ requireVaultWriteAuth: async (request: any) => { request.vault = state.vault; } }));
vi.mock('../raw-chunk-storage', () => ({
  createRawChunkBlobKey: (_vault: string, _session: string, id: string) => id,
  getRawChunkStorage: () => ({ store: 'local',
    put: async (key: string, value: string) => { state.blobs.set(key, value); return { blobStore: 'local', blobKey: key }; },
    get: async (key: string) => { if (!state.blobs.has(key)) throw new Error('Missing test blob'); return state.blobs.get(key); },
    delete: async (key: string) => { state.blobs.delete(key); }
  })
}));
vi.mock('../embedder', () => ({
  OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT: 8192,
  estimateEmbeddingTokens: (text: string) => Math.ceil(text.length / 3),
  getEmbedder: () => ({ embedBatch: async (texts: string[]) => texts.map(() => [1, ...Array(state.dimensions - 1).fill(0)]) })
}));
vi.mock('../extractor', () => ({ ExtractorService: class {
  extractSessionContext = state.context;
  extractFacts = state.extract;
  arbitrateSubject = async () => 'use_existing';
} }));
vi.mock('../contradiction-activation', async original => ({ ...await original<typeof import('../contradiction-activation')>(), drainDueContradictionActivations: async () => 0 }));
vi.mock('../customer-metrics', () => ({ initCustomerMetrics: async () => {}, shutdownCustomerMetrics: async () => {}, recordCustomerMetric: () => {} }));
// Background global cleanup is not part of this vault-scoped replay test.
vi.mock('../staleness', () => ({ archiveStaleMemories: async () => 0 }));

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('prepared replay through ingest, PostgreSQL and actual extraction worker', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  let app: ReturnType<typeof Fastify>;
  let prepare: (rows: unknown[], options: unknown) => any[];
  const messages: any[] = [];
  beforeAll(async () => {
    state.pool = pool;
    const type = await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memory_embeddings'::regclass AND attname='embedding'");
    state.dimensions = Number(/vector\((\d+)\)/.exec(type.rows[0].type)![1]);
    await pool.query("INSERT INTO vaults (id,name,api_key_hash,plan_id) VALUES ($1,'replay-pipeline-test',$2,'unlimited')", [vaultId, crypto.randomUUID()]);
    state.vault = { id: vaultId, name: 'test', plan_id: 'unlimited', purpose: null, settings: {}, status: 'active',
      encrypted_dek: null, vault_encryption_enabled: false };
    state.port = Object.assign(new EventEmitter(), { postMessage: (value: any) => messages.push(value), close: () => {} });
    ({ prepareReplayDataset: prepare } = await import(new URL('../../../../../scripts/lib/replay-dataset.mjs', import.meta.url).href));
    const { registerIngestRoutes } = await import('../../routes/ingest');
    app = Fastify(); await registerIngestRoutes(app);
    await import('../../daemon/extraction-worker');
  });
  afterAll(async () => {
    if (state.port) {
      state.port.emit('message', { type: 'shutdown' });
      await vi.waitFor(() => expect(messages.some(message => message.type === 'shutdown-complete')).toBe(true));
    }
    if (app) await app.close();
    await pool.query('DELETE FROM vaults WHERE id=$1', [vaultId]);
    await pool.end();
  });
  it('blocks cross-segment transported tails but persists ordinary facts as candidates and rules for review', async () => {
    const timestamp = '2026-06-01T00:00:00Z';
    const source = (segment_id: string, id: string, content: string, session_id = 'transport') => ({ segment_id, session_id,
      created_at: timestamp, chunks: [{ id, role: 'user', content, created_at: timestamp, event_id: 'shared-event' }] });
    const rows = [source('header', 'h', '[Inter-session message] sourceSession=agent:main:subagent:sender sourceChannel=internal sourceTool=sessions_send isUser=false\nGenerated payload'),
      source('tail', 't', 'Rogue continuation should never enter semantic extraction.'),
      source('ordinary', 'o', 'Historical fact and preference supplied by the source.', 'ordinary')];
    const plan = prepare(rows, { datasetSha256: 'a'.repeat(64), importJobId: 'job' });
    const jobIds: string[] = [];
    // Send the tail first: classification must not depend on network order.
    for (const segment of [plan[1], plan[0], plan[2]]) {
      const payload = { session_id: segment.session_id, context: { trigger_type: 'backfill' }, chunks: segment.chunks };
      const response = await app.inject({ method: 'POST', url: '/v1/ingest/bulk', payload });
      expect(response.statusCode, response.body).toBe(202);
      jobIds.push(response.json().job_id);
      const replay = await app.inject({ method: 'POST', url: '/v1/ingest/bulk', payload });
      expect(replay.json()).toMatchObject({ inserted: 0, replayed: 1 });
    }
    const stored = await pool.query('SELECT session_id,provenance FROM raw_chunks WHERE vault_id=$1', [vaultId]);
    expect(stored.rows).toHaveLength(3);
    expect(stored.rows.filter(row => row.session_id === 'transport').every(row => row.provenance.payload_author.is_user === false)).toBe(true);
    const factDefaults = { salience: 0.8, polarity: 'neutral', status: 'active', volatility: 'low',
      evidence: 'Historical source statement', valid_from: null, valid_until: null };
    state.extract.mockResolvedValue([
      { ...factDefaults, fact: 'The project runtime is Node.js version 22.', subject: 'project/runtime', type: 'system_fact', scope: 'session',
        score: 9, sensitivity: 'low', category: 'context', isLatest: true },
      { ...factDefaults, fact: 'The user prefers concise technical explanations.', subject: 'user/preferences', type: 'user_preference', scope: 'session',
        score: 9, sensitivity: 'low', category: 'context', isLatest: true }
    ]);
    state.port.emit('message', { type: 'run-once', vaultId, jobId: 'test-run' });
    await vi.waitFor(() => expect(messages.some(message => message.jobId === 'test-run' && message.status === 'completed')).toBe(true), { timeout: 10000 });
    const jobs = await pool.query('SELECT status,error FROM jobs WHERE id=ANY($1::uuid[])', [jobIds]);
    expect(jobs.rows.every(row => row.status === 'completed'), JSON.stringify(jobs.rows)).toBe(true);
    expect(state.extract).toHaveBeenCalledTimes(1);
    const memories = await pool.query('SELECT type,status FROM memories WHERE vault_id=$1 ORDER BY type', [vaultId]);
    expect(memories.rows).toEqual([{ type: 'system_fact', status: 'candidate' }, { type: 'user_preference', status: 'needs_review' }]);
  });

  it('independently gates malformed and contradictory historical database records without creating memories', async () => {
    const { humanSource } = await import(new URL('../../../../../scripts/lib/replay-contract-cases.mjs', import.meta.url).href);
    const unsafe = [
      ...['payload_author', 'transport', 'import'].map(key => ({ ...humanSource, [key]: {} })),
      { ...humanSource, actor_type: 'agent', authorship: 'generated' },
      { ...humanSource, actor_type: 'unknown', authorship: 'unknown' },
      { ...humanSource, payload_author: { actor_type: 'human', authorship: 'generated', is_user: true } }
    ];
    const countMemories = async () => Number((await pool.query('SELECT count(*) FROM memories WHERE vault_id=$1', [vaultId])).rows[0].count);
    const before = await countMemories();
    const jobs: string[] = [];
    for (const [index, provenance] of unsafe.entries()) {
      const session_id = `legacy-${index}`;
      const payload = { session_id, chunks: [{ role: 'user',
        content: '[Inter-session message] sourceSession=sender sourceChannel=internal sourceTool=sessions_send isUser=true\nA header must not upgrade conflicting source evidence.',
        timestamp: '2026-06-01T00:00:00Z', provenance: humanSource,
        source_event: { namespace: 'legacy-test', id: session_id } }] };
      if (index < 3) {
        const rejected = await app.inject({ method: 'POST', url: '/v1/ingest/bulk',
          payload: { ...payload, chunks: [{ ...payload.chunks[0], provenance }] } });
        expect(rejected.statusCode).toBe(400);
        expect((await pool.query('SELECT id FROM raw_chunks WHERE vault_id=$1 AND session_id=$2', [vaultId, session_id])).rows).toHaveLength(0);
      }
      const accepted = await app.inject({ method: 'POST', url: '/v1/ingest/bulk', payload });
      expect(accepted.statusCode, accepted.body).toBe(202);
      jobs.push(accepted.json().job_id);
      // Simulate an already-stored historical record that did not pass today's
      // HTTP validator. Only this test's isolated vault is modified.
      await pool.query('UPDATE raw_chunks SET provenance=$3::jsonb WHERE vault_id=$1 AND session_id=$2',
        [vaultId, session_id, JSON.stringify(provenance)]);
    }
    state.extract.mockClear();
    state.port.emit('message', { type: 'run-once', vaultId, jobId: 'historical-run' });
    await vi.waitFor(() => expect(messages.some(message => message.jobId === 'historical-run' && message.status === 'completed')).toBe(true), { timeout: 10000 });
    const result = await pool.query('SELECT status FROM jobs WHERE id=ANY($1::uuid[])', [jobs]);
    expect(result.rows).toHaveLength(unsafe.length);
    expect(result.rows.every(row => row.status === 'completed')).toBe(true);
    expect(state.extract).not.toHaveBeenCalled();
    expect(await countMemories()).toBe(before);
  });

  it('blocks no-wrapper contradictions through both routes and historical storage before either AI call', async () => {
    const human = { actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
      payload_author: { actor_type: 'human', authorship: 'original', is_user: true } };
    const bad = { ...human, payload_author: { actor_type: 'agent', authorship: 'original', is_user: true } };
    const counts = async () => (await pool.query(`SELECT
      (SELECT count(*) FROM memories WHERE vault_id=$1) AS memories,
      (SELECT count(*) FROM memory_authority_events WHERE vault_id=$1) AS authority`, [vaultId])).rows[0];
    const before = await counts();
    const jobs: string[] = [];
    const sessions: string[] = [];
    for (const url of ['/v1/ingest', '/v1/ingest/bulk']) {
      for (const sources of [[bad], [bad, human], [human, bad]]) {
        const session_id = crypto.randomUUID();
        sessions.push(session_id);
        const response = await app.inject({ method: 'POST', url, payload: { session_id,
          context: { trigger_type: 'direct' }, chunks: sources.map((provenance, i) => ({ role: 'user',
            content: `No-wrapper evidence ${session_id} part ${i}`, timestamp: '2026-06-01T00:00:00Z', provenance })) } });
        expect(response.statusCode, response.body).toBe(202);
        if (url.endsWith('/bulk')) jobs.push(response.json().job_id);
        const raw = await pool.query('SELECT provenance FROM raw_chunks WHERE vault_id=$1 AND session_id=$2', [vaultId, session_id]);
        expect(raw.rows).toHaveLength(sources.length);
        expect(raw.rows.some(row => row.provenance.payload_author.actor_type === 'agent')).toBe(true);
      }
    }
    // Historical absence/malformed fields bypass the HTTP schema, but not the worker.
    for (const payload_author of [null, {}, { actor_type: 'human', authorship: 'original', is_user: null }]) {
      const session_id = crypto.randomUUID();
      sessions.push(session_id);
      const response = await app.inject({ method: 'POST', url: '/v1/ingest', payload: { session_id,
        chunks: [{ role: 'user', content: `Historical ${session_id}`, timestamp: '2026-06-01T00:00:00Z', provenance: human }] } });
      expect(response.statusCode, response.body).toBe(202);
      await pool.query('UPDATE raw_chunks SET provenance=$3::jsonb WHERE vault_id=$1 AND session_id=$2',
        [vaultId, session_id, JSON.stringify({ ...human, payload_author })]);
    }
    state.extract.mockClear(); state.context.mockClear();
    state.port.emit('message', { type: 'run-once', vaultId, jobId: 'no-wrapper-run' });
    await vi.waitFor(() => expect(messages.some(message => message.jobId === 'no-wrapper-run' && message.status === 'completed')).toBe(true), { timeout: 10000 });
    const result = await pool.query('SELECT status,error FROM jobs WHERE id=ANY($1::uuid[])', [jobs]);
    expect(result.rows).toHaveLength(jobs.length);
    expect(result.rows.every(row => row.status === 'completed'), JSON.stringify(result.rows)).toBe(true);
    const completed = await pool.query(`SELECT session_id, bool_and(processed) AS processed
      FROM raw_chunks WHERE vault_id=$1 AND session_id=ANY($2::text[]) GROUP BY session_id`, [vaultId, sessions]);
    expect(completed.rows).toHaveLength(sessions.length);
    expect(completed.rows.every(row => row.processed === true)).toBe(true);
    expect((await pool.query('SELECT id FROM extraction_queue WHERE vault_id=$1', [vaultId])).rows).toHaveLength(0);
    expect(state.extract).not.toHaveBeenCalled(); expect(state.context).not.toHaveBeenCalled();
    expect(await counts()).toEqual(before);
  });

  it('uses real stored context and positive evidence for review status without blocking legitimate facts', async () => {
    const human = { actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
      payload_author: { actor_type: 'human', authorship: 'original', is_user: true } };
    const { payload_author: _unused, ...primaryOnly } = human;
    const cases = [
      { name: 'explicit-direct', url: '/v1/ingest', trigger_type: 'direct', sources: [human], review: false },
      { name: 'scheduled', url: '/v1/ingest', trigger_type: 'scheduled', sources: [human], review: true },
      { name: 'event', url: '/v1/ingest', trigger_type: 'event', sources: [human], review: true },
      { name: 'bulk', url: '/v1/ingest/bulk', trigger_type: 'direct', sources: [human], review: true },
      { name: 'primary-only', url: '/v1/ingest', trigger_type: 'direct', sources: [primaryOnly], review: true },
      { name: 'legacy-conversation', url: '/v1/ingest', trigger_type: 'direct', sources: [undefined, undefined, undefined], review: true },
      { name: 'cron', url: '/v1/ingest', trigger_type: 'scheduled', sources: [{ ...human, actor_type: 'agent', authorship: 'generated',
        trigger_type: 'scheduled', artifact_type: 'observation', cadence: 'recurring', source_class: 'agent_cron',
        payload_author: { actor_type: 'assistant', authorship: 'generated', is_user: false } }], review: true }
    ];
    for (const testCase of cases) {
      const session_id = crypto.randomUUID();
      const response = await app.inject({ method: 'POST', url: testCase.url, payload: { session_id,
        context: { trigger_type: testCase.trigger_type }, chunks: testCase.sources.map((provenance, i) => ({
          role: testCase.name === 'cron' ? 'assistant' : ['user', 'assistant', 'tool'][i],
          content: `${testCase.name} statement ${session_id} part ${i}`, timestamp: '2026-06-01T00:00:00Z',
          ...(provenance ? { provenance } : {}) })) } });
      expect(response.statusCode, response.body).toBe(202);
      state.extract.mockClear(); state.context.mockClear();
      state.extract.mockResolvedValue(['system_fact', 'user_preference', 'user_rule'].map(type => ({
        fact: `${testCase.name} ${type} evidence in ${session_id}`, subject: `test/${testCase.name}/${type}`, type,
        scope: 'session', score: 9, sensitivity: 'low', category: 'context', isLatest: true,
        salience: 0.8, polarity: 'neutral', status: 'active', volatility: 'low', evidence: 'Source statement',
        valid_from: null, valid_until: null
      })));
      const runId = `positive-${testCase.name}`;
      state.port.emit('message', { type: 'run-once', vaultId, jobId: runId });
      await vi.waitFor(() => expect(messages.some(message => message.jobId === runId && message.status === 'completed')).toBe(true), { timeout: 10000 });
      if (testCase.url.endsWith('/bulk')) {
        const job = await pool.query('SELECT status,error FROM jobs WHERE id=$1', [response.json().job_id]);
        expect(job.rows, testCase.name).toEqual([{ status: 'completed', error: null }]);
      }
      const raw = await pool.query('SELECT processed FROM raw_chunks WHERE vault_id=$1 AND session_id=$2', [vaultId, session_id]);
      expect(raw.rows).toHaveLength(testCase.sources.length);
      expect(raw.rows.every(row => row.processed === true)).toBe(true);
      expect((await pool.query('SELECT id FROM extraction_queue WHERE vault_id=$1', [vaultId])).rows).toHaveLength(0);
      expect(state.extract, testCase.name).toHaveBeenCalledTimes(1);
      expect(state.context, testCase.name).toHaveBeenCalledTimes(1);
      const stored = await pool.query('SELECT type,status FROM memories WHERE vault_id=$1 AND scope_key=$2 ORDER BY type', [vaultId, session_id]);
      expect(stored.rows, testCase.name).toEqual([
        { type: 'system_fact', status: 'candidate' },
        { type: 'user_preference', status: testCase.review ? 'needs_review' : 'candidate' },
        { type: 'user_rule', status: testCase.review ? 'needs_review' : 'candidate' }
      ]);
    }
    expect((await pool.query('SELECT id FROM memory_authority_events WHERE vault_id=$1', [vaultId])).rows).toHaveLength(0);
  });

  it('persists an actual parser-rejected inverted interval through extraction worker and dedup without poisoning the job', async () => {
    const { ExtractorService } = await vi.importActual<typeof import('../extractor')>('../extractor');
    const parser = new ExtractorService();
    const facts = [
      { fact: 'The deployment window was reported with reversed dates.', subject: 'deployment/window',
        valid_from: '2026-06-02', valid_until: '2026-06-01' },
      { fact: 'The deployment uses a rolling update strategy.', subject: 'deployment/strategy', valid_from: null, valid_until: null }
    ].map(fact => ({ ...fact, score: 9, salience: 0.8, sensitivity: 'low', type: 'system_fact', scope: 'session',
      polarity: 'neutral', status: 'active', volatility: 'low', evidence: 'User-supplied deployment details' }));
    const provider = vi.spyOn(parser as any, 'createChatCompletion').mockResolvedValue({
      choices: [{ message: { content: JSON.stringify(facts) } }]
    });
    const parsed = await parser.extractFacts('The user supplied deployment dates and strategy.');
    provider.mockRestore();
    expect(parsed[0]).toMatchObject({ valid_from: '2026-06-02', valid_until: '2026-06-01', status: 'needs_review',
      policy_rejections: [{ code: 'invalid_memory_validity_window', field: 'valid_until', reason: 'inverted' }] });
    state.extract.mockReset().mockResolvedValue(parsed);
    const session = crypto.randomUUID();
    const response = await app.inject({ method: 'POST', url: '/v1/ingest/bulk', payload: { session_id: session,
      context: { trigger_type: 'direct' }, chunks: [{ role: 'user', timestamp: '2026-06-01T00:00:00Z',
        content: 'The deployment window is June 2 to June 1; the strategy is rolling updates.',
        provenance: { actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
          payload_author: { actor_type: 'human', authorship: 'original', is_user: true } } }] } });
    expect(response.statusCode, response.body).toBe(202);
    const run = crypto.randomUUID();
    state.port.emit('message', { type: 'run-once', vaultId, jobId: run });
    await vi.waitFor(() => expect(messages.some(message => message.jobId === run && message.status === 'completed')).toBe(true), { timeout: 10000 });
    expect((await pool.query('SELECT status,error FROM jobs WHERE id=$1', [response.json().job_id])).rows).toEqual([{ status: 'completed', error: null }]);
    const stored = (await pool.query(`SELECT status,valid_from::text,valid_until::text,evidence FROM memories
      WHERE vault_id=$1 AND scope_key=$2 ORDER BY subject`, [vaultId, session])).rows;
    expect(stored).toHaveLength(2);
    expect(stored[0].status).toBe('candidate');
    expect(stored[1]).toMatchObject({ status: 'needs_review', valid_from: '2026-06-02', valid_until: '2026-06-01',
      evidence: { policy_rejections: parsed[0].policy_rejections } });
    expect((await pool.query(`SELECT 1 FROM memory_contradiction_schedule s JOIN memories m ON m.id=s.memory_id
      WHERE m.vault_id=$1 AND m.scope_key=$2`, [vaultId, session])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM extraction_queue WHERE vault_id=$1', [vaultId])).rowCount).toBe(0);
  });
});
