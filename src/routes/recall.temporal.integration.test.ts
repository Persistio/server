import crypto from 'node:crypto';
import Fastify, { type FastifyRequest } from 'fastify';
import { Pool, type QueryResultRow } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({
  vault: null as any, vector: [] as number[], unwrap: vi.fn(),
  blobs: new Map<string, string>(), readBlob: vi.fn()
}));
vi.mock('../middleware/auth', () => ({
  requireVaultReadAuth: async (request: FastifyRequest) => { request.vault = state.vault; }
}));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed: async () => state.vector }) }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = state.unwrap; } }));
vi.mock('../services/raw-chunk-storage', () => ({
  getRawChunkStorage: () => ({ store: 'local', get: state.readBlob })
}));

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
// Per-connection startup options keep this test non-UTC even on UTC CI servers;
// they do not mutate the shared database/server configuration.
const connectionString = databaseUrl ? (() => {
  const url = new URL(databaseUrl);
  url.searchParams.set('options', [url.searchParams.get('options'), '-c timezone=Pacific/Honolulu'].filter(Boolean).join(' '));
  return url.toString();
})() : undefined;
// Deliberately independent of the implementation's exported arrays: these are
// the nine product types in prompts/extractor.txt, not the two human-intent types
// and not ranking boosts (factual mode boosts decisions, but cannot revive them).
const behavioral = ['user_rule', 'user_preference', 'task_pattern', 'workflow', 'constraint', 'decision'];
const factual = ['project', 'system_fact', 'domain_knowledge'];
const instant = '2026-09-19T00:05:00.000Z';
const today = '2026-09-19', yesterday = '2026-09-18', tomorrow = '2026-09-20';
const surfaces = [
  { mode: 'agent', format: 'json' }, { mode: 'factual', format: 'json' },
  { mode: 'agent', format: 'bundle_v3' }, { mode: 'factual', format: 'bundle_v3' }
] as const;
type Phase = 'semantic' | 'graph' | 'final' | 'counter';
type Scope = 'global' | 'session' | 'project' | 'task';
interface Fixture {
  type?: string | null; data?: string; subject?: string; scope?: Scope; key?: string | null;
  from?: string | null; until?: string | null; vector?: number[]; salience?: number;
  status?: 'active' | 'superseded' | 'contradicted'; archived?: boolean;
  sources?: string[]; encryptedSubject?: string | null;
}

// Auth/provider boundaries are synthetic; migrations, eligibility, pgvector,
// graph traversal, revision triggers, counters and all query results are real.
// Never disable production constraints to insert otherwise impossible states.
describe.skipIf(!databaseUrl)('temporal recall eligibility (real PostgreSQL)', () => {
  const pool = new Pool({ connectionString }), app = Fastify();
  let db: typeof import('../db/client');
  let config: ReturnType<typeof import('../config')['getConfig']>;
  let crypt: typeof import('../services/crypto');
  const before = new Map<Phase, () => Promise<void>>();
  const after = new Map<Phase, () => Promise<void>>();
  const calls: { phase: Phase; args: unknown[]; ids: string[]; revisions: string[] }[] = [];
  const counters: Promise<unknown>[] = [];
  function phaseOf(sql: string): Phase | undefined {
    if (sql.includes('WITH targets AS MATERIALIZED')) return 'counter';
    if (sql.includes('WITH selected AS')) return 'final';
    if (sql.includes('FROM memories m JOIN memory_embeddings')) return 'semantic';
    if (sql.includes('SELECT DISTINCT') && sql.includes('FROM memory_edges e JOIN memories m')) return 'graph';
  }
  beforeAll(async () => {
    process.env.DATABASE_URL = connectionString;
    db = await import('../db/client');
    await db.runMigrations();
    config = (await import('../config')).getConfig();
    config.ENCRYPTION_ENABLED = false;
    const column = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    state.vector = [1, ...Array(Number(/\((\d+)\)/.exec(column)![1]) - 1).fill(0)];
    crypt = await import('../services/crypto');
    // Import only after DATABASE_URL is set: this module transitively creates
    // the production pool, whose startup options must match the fixture pool.
    (await import('../http-error-handler')).registerPlatformErrorHandler(app);
    await (await import('./recall')).registerRecallRoutes(app);
  });
  beforeEach(async () => {
    // Only Date is faked; socket/timer behavior and PostgreSQL's clock stay real.
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date(instant));
    config.ENCRYPTION_ENABLED = false;
    before.clear(); after.clear(); calls.length = 0; counters.length = 0;
    state.blobs.clear(); state.unwrap.mockReset();
    state.readBlob.mockReset().mockImplementation(async (key: string) => {
      if (!state.blobs.has(key)) throw new Error('Unexpected source read: ' + key);
      return state.blobs.get(key)!;
    });
    const id = crypto.randomUUID();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'temporal-recall-regression',$2,'unlimited')", [id, crypto.randomUUID()]);
    state.vault = (await pool.query('SELECT * FROM vaults WHERE id=$1', [id])).rows[0];
    const realQuery = db.query;
    vi.spyOn(db, 'query').mockImplementation(async <T extends QueryResultRow = QueryResultRow>(sql: string, args?: unknown[]) => {
      const phase = phaseOf(sql);
      if (phase && before.has(phase)) {
        const hook = before.get(phase)!; before.delete(phase); await hook();
      }
      const pending = realQuery<T>(sql, args);
      if (phase === 'counter') counters.push(pending);
      const result = await pending;
      if (phase) {
        calls.push({ phase, args: args ?? [], ids: result.rows.map(row => row.id), revisions: result.rows.map(row => row.revision) });
        if (after.has(phase)) {
          const hook = after.get(phase)!; after.delete(phase); await hook();
        }
      }
      return result;
    });
  });
  afterEach(async () => {
    // Counter writes are fire-and-forget in the route. Drain actual promises,
    // rather than sleeping/racing cleanup or asserting stale database state.
    await Promise.all(counters);
    vi.restoreAllMocks(); vi.useRealTimers();
    config.ENCRYPTION_ENABLED = false;
    await pool.query('DELETE FROM vaults WHERE id=$1', [state.vault.id]);
  });
  afterAll(async () => { await app.close(); await pool.end(); await db?.closePool(); });

  const orthogonal = () => [0, 1, ...state.vector.slice(2)];
  async function memory(options: Fixture = {}) {
    const id = crypto.randomUUID(), data = options.data ?? `Synthetic ${options.type ?? 'fact'} ${id}`;
    const scope = options.scope ?? 'session';
    const result = await pool.query(
      `INSERT INTO memories(id,vault_id,data,subject,subject_encrypted,hash,status,type,scope,scope_key,
         valid_from,valid_until,source_chunks,embedding,salience,archived_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::date,$12::date,$13::uuid[],$14::vector,$15,
         CASE WHEN $16::boolean THEN now() ELSE NULL END) RETURNING revision::text`,
      [id, state.vault.id, data, options.subject ?? 'Temporal regression', options.encryptedSubject ?? null,
        crypto.randomUUID(), options.status ?? 'active', options.type === undefined ? 'system_fact' : options.type,
        scope, options.key === undefined ? (scope === 'global' ? null : 's1') : options.key,
        options.from ?? null, options.until ?? null, options.sources ?? [],
        JSON.stringify(options.vector ?? state.vector), options.salience ?? 0.5, options.archived ?? false]);
    await pool.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)', [id, JSON.stringify(options.vector ?? state.vector)]);
    return { id, data, revision: result.rows[0].revision as string };
  }
  async function edge(from: string, to: string) {
    await pool.query("INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence) VALUES($1,$2,$3,'supports',0.9)", [state.vault.id, from, to]);
  }
  async function source(content: string) {
    const id = crypto.randomUUID(); state.blobs.set(id, content);
    await pool.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,capture_context)
      VALUES($1::uuid,$2,'s1','user','local',$1::text,$3,'{}'::jsonb)`, [id, state.vault.id, Buffer.byteLength(content)]);
    return id;
  }
  async function recall(body: Record<string, unknown> = {}, format = 'json') {
    const result = await app.inject({ method: 'POST', url: '/v1/recall' + (format === 'json' ? '' : '?format=' + format),
      payload: { query: 'Temporal regression', context: { session_id: 's1' }, min_similarity: 0.5, top_k: 100,
        max_bundle_bytes: 65536, ...body } });
    expect(result.statusCode, result.body).toBe(200);
    await Promise.all(counters);
    return result.json();
  }
  function records(response: any, format: string): any[] {
    return format === 'json' ? [...response.memories, ...response.related_memories]
      : response.bundle.split('\n').filter((line: string) => line.startsWith('{')).map((line: string) => JSON.parse(line));
  }
  const contentOf = (row: any) => row.data ?? row.memory;
  async function counts() {
    return (await pool.query('SELECT id,recall_count,revision::text,last_recalled IS NOT NULL AS recalled FROM memories WHERE vault_id=$1 ORDER BY id', [state.vault.id])).rows;
  }
  const observed = (phase: Phase) => calls.filter(call => call.phase === phase);

  it('runs on PostgreSQL 17 and preserves non-UTC database session timezone', async () => {
    const version = (await pool.query('SHOW server_version_num')).rows[0].server_version_num;
    expect(Number(version)).toBeGreaterThanOrEqual(170000); expect(Number(version)).toBeLessThan(180000);
    // Both pools explicitly use non-UTC startup options regardless of CI defaults.
    expect((await pool.query('SHOW TimeZone')).rows[0].TimeZone).toBe('Pacific/Honolulu');
    expect((await db.query('SHOW TimeZone')).rows[0].TimeZone).toBe('Pacific/Honolulu');
  });

  it.each(behavioral.flatMap(type => surfaces.map(surface => ({ type, ...surface }))))(
    'filters every action-guiding window for $type, $mode / $format with inclusive UTC endpoints',
    async ({ type, mode, format }) => {
      const keep = [
        await memory({ type }), await memory({ type, from: today }), await memory({ type, until: today }),
        await memory({ type, from: today, until: today }), await memory({ type, from: yesterday, until: tomorrow })
      ];
      const drop = [await memory({ type, until: yesterday }), await memory({ type, from: tomorrow })];
      const result = records(await recall({ mode }, format), format);
      expect(result.map(contentOf).sort()).toEqual(keep.map(row => row.data).sort());
      expect(result.every(row => row.temporal === 'current_or_unbounded')).toBe(true);
      expect(observed('semantic')[0].ids.sort()).toEqual(keep.map(row => row.id).sort());
      for (const phase of ['semantic', 'final'] as const) expect(observed(phase)[0].args[4]).toBe(instant);
      const metadata = await counts();
      for (const row of metadata) expect(row).toMatchObject({ recall_count: drop.some(m => m.id === row.id) ? 0 : 1, revision: '1' });
    }
  );

  it.each(factual.flatMap(type => surfaces.map(surface => ({ type, ...surface }))))(
    'retains historical and future factual knowledge for $type, $mode / $format',
    async ({ type, mode, format }) => {
      const old = await memory({ type, from: '2020-01-01', until: '2020-12-31' });
      const future = await memory({ type, from: tomorrow });
      const current = await memory({ type, from: today, until: today });
      const unbounded = await memory({ type });
      const result = records(await recall({ mode }, format), format);
      expect(result.map(contentOf).sort()).toEqual([old, future, current, unbounded].map(row => row.data).sort());
      const temporal = new Map(result.map(row => [contentOf(row), row.temporal]));
      expect(temporal.get(old.data)).toBe('historical'); expect(temporal.get(future.data)).toBe('future');
      expect(temporal.get(current.data)).toBe('current_or_unbounded'); expect(temporal.get(unbounded.data)).toBe('current_or_unbounded');
    }
  );

  it.each(surfaces)('excludes null types and representable invalid date ranges before budgeting, $mode / $format', async ({ mode, format }) => {
    const denied = [
      await memory({ type: null }), await memory({ from: '-infinity' }), await memory({ until: 'infinity' }),
      await memory({ from: '0001-01-01 BC' }), await memory({ until: '10000-01-01' })
    ];
    const keep = await memory({ type: 'system_fact' });
    const result = records(await recall({ mode, top_k: 1 }, format), format);
    expect(result.map(contentOf)).toEqual([keep.data]);
    expect(observed('semantic')[0].ids).toEqual([keep.id]);
    for (const row of await counts()) expect(row.recall_count).toBe(denied.some(m => m.id === row.id) ? 0 : 1);
  });

  it('leaves production constraints and revision triggers active for impossible stored metadata', async () => {
    for (const options of [
      { type: 'unknown_type' }, { from: '2026-02-30' }, { from: tomorrow, until: yesterday }
    ]) await expect(memory(options)).rejects.toThrow();
    expect((await counts()).length).toBe(0);
    const m = await memory();
    await pool.query("UPDATE memories SET valid_until=$2 WHERE id=$1", [m.id, yesterday]);
    expect((await counts())[0].revision).toBe('2');
  });

  it.each(['global', 'session', 'project', 'task'] as const)('combines temporal eligibility with exact %s scope and namespace', async scope => {
    const key = scope === 'global' ? null : 'same-text';
    const keep = await memory({ type: 'constraint', scope, key, until: today });
    const expired = await memory({ type: 'constraint', scope, key, until: yesterday });
    const contexts = [
      {}, { session_id: 'same-text' }, { project_id: 'same-text' }, { task_id: 'same-text' },
      { session_id: 'other', project_id: 'other', task_id: 'other' }
    ];
    for (const context of contexts) {
      const expected = scope === 'global' || (context as Record<string, string>)[scope + '_id'] === 'same-text';
      for (const format of ['json', 'bundle_v3']) {
        const result = records(await recall({ context }, format), format);
        expect(result.map(contentOf)).toEqual(expected ? [keep.data] : []);
        expect(result.map(contentOf)).not.toContain(expired.data);
      }
    }
  });

  it.each(surfaces)('excludes temporal rows before the semantic candidate LIMIT and top_k, $mode / $format', async ({ mode, format }) => {
    // top_k=1 has a 25-row candidate cap. Thirty exact-vector matches must not
    // starve a weaker eligible match, regardless of ranking-mode boosts.
    for (let i = 0; i < 30; i++) await memory({ type: behavioral[i % behavioral.length],
      ...(i % 2 ? { from: tomorrow } : { until: yesterday }), salience: 1 });
    const keep = await memory({ type: 'system_fact', until: '2020-12-31', vector: [0.8, 0.6, ...state.vector.slice(2)] });
    const result = records(await recall({ mode, top_k: 1 }, format), format);
    expect(result.map(contentOf)).toEqual([keep.data]);
    expect(observed('semantic')[0].ids).toEqual([keep.id]);
    expect((await counts()).filter(row => row.recall_count)).toMatchObject([{ id: keep.id, recall_count: 1 }]);
  });

  it.each(surfaces)('filters graph destinations before its LIMIT and shares one top_k, $mode / $format', async ({ mode, format }) => {
    const seed = await memory({ type: 'workflow', until: today });
    for (const type of behavioral) {
      for (const window of [{ until: yesterday }, { from: tomorrow }]) {
        const denied = await memory({ type, ...window, vector: orthogonal(), salience: 1 });
        await edge(seed.id, denied.id);
      }
    }
    const neighbor = await memory({ type: 'decision', from: today, until: today, vector: orthogonal(), salience: 0.1 });
    await edge(seed.id, neighbor.id);
    const response = await recall({ mode, top_k: 2 }, format);
    expect(records(response, format).map(contentOf)).toEqual([seed.data, neighbor.data]);
    expect(observed('graph')[0].ids).toEqual([neighbor.id]);
    expect(observed('graph')[0].args[4]).toBe(instant);
    if (format === 'json') {
      expect(response.memories.map((row: any) => row.id)).toEqual([seed.id]);
      expect(response.related_memories).toMatchObject([{ id: neighbor.id, source: 'graph', temporal: 'current_or_unbounded' }]);
    }
    const noRoom = await recall({ mode, top_k: 1 }, format);
    expect(records(noRoom, format).map(contentOf)).toEqual([seed.data]);
    expect(observed('graph')).toHaveLength(1);
  });

  it('excludes null types, invalid dates and inactive graph destinations before its budget', async () => {
    const seed = await memory();
    for (const options of [
      { type: null }, { until: 'infinity' }, { from: '-infinity' },
      { status: 'superseded' as const }, { status: 'contradicted' as const }, { archived: true }
    ]) {
      const denied = await memory({ ...options, vector: orthogonal(), salience: 1 }); await edge(seed.id, denied.id);
    }
    const neighbor = await memory({ type: 'constraint', vector: orthogonal(), salience: 0.1 }); await edge(seed.id, neighbor.id);
    const response = await recall({ top_k: 2 });
    expect(response.related_memories.map((row: any) => row.id)).toEqual([neighbor.id]);
    expect(observed('graph')[0].ids).toEqual([neighbor.id]);
    expect((await counts()).filter(row => row.recall_count).map(row => row.id).sort()).toEqual([seed.id, neighbor.id].sort());
  });

  it.each(factual)('permits historical/future %s graph destinations without treating them as current', async type => {
    const seed = await memory();
    const old = await memory({ type, until: yesterday, vector: orthogonal() });
    const future = await memory({ type, from: tomorrow, vector: orthogonal() });
    await edge(seed.id, old.id); await edge(seed.id, future.id);
    const response = await recall({ top_k: 3 });
    expect(response.related_memories.map((row: any) => row.id).sort()).toEqual([old.id, future.id].sort());
    expect(response.related_memories.find((row: any) => row.id === old.id).temporal).toBe('historical');
    expect(response.related_memories.find((row: any) => row.id === future.id).temporal).toBe('future');
  });

  it.each(['global', 'session', 'project', 'task'] as const)('requires graph origin and destination to have the same exact %s binding', async scope => {
    const key = scope === 'global' ? null : 'same-text';
    const seed = await memory({ scope, key });
    const neighbor = await memory({ scope, key, type: 'constraint', until: today, vector: orthogonal() });
    await edge(seed.id, neighbor.id);
    for (const otherScope of ['global', 'session', 'project', 'task'] as const) {
      if (otherScope === scope) continue;
      const other = await memory({ scope: otherScope, key: otherScope === 'global' ? null : 'same-text', vector: orthogonal() });
      await edge(seed.id, other.id);
    }
    if (scope !== 'global') {
      const otherKey = await memory({ scope, key: 'other-key', vector: orthogonal() });
      await edge(seed.id, otherKey.id);
    }
    const response = await recall({ context: { session_id: 'same-text', project_id: 'same-text', task_id: 'same-text' } });
    expect(response.memories.map((row: any) => row.id)).toEqual([seed.id]);
    expect(response.related_memories.map((row: any) => row.id)).toEqual([neighbor.id]);
  });

  it.each(behavioral)('never uses an expired/future %s as a graph origin', async type => {
    const current = await memory({ type, from: today, until: today });
    const currentNeighbor = await memory({ vector: orthogonal() }); await edge(current.id, currentNeighbor.id);
    const deniedNeighbors: string[] = [];
    for (const window of [{ until: yesterday }, { from: tomorrow }]) {
      const origin = await memory({ type, ...window });
      const neighbor = await memory({ vector: orthogonal() }); await edge(origin.id, neighbor.id);
      deniedNeighbors.push(neighbor.id);
    }
    const response = await recall();
    expect(response.memories.map((row: any) => row.id)).toEqual([current.id]);
    expect(response.related_memories.map((row: any) => row.id)).toEqual([currentNeighbor.id]);
    expect(observed('graph')[0].args[5]).toEqual([current.id]);
    for (const id of deniedNeighbors) expect((await counts()).find(row => row.id === id)?.recall_count).toBe(0);
  });

  const mutations = [
    { name: 'expiration', sql: 'valid_until=$2', value: yesterday },
    { name: 'future start', sql: 'valid_from=$2', value: tomorrow },
    { name: 'null type', sql: 'type=$2', value: null },
    { name: 'different exact binding', sql: 'scope_key=$2', value: 'different' },
    { name: 'superseded status', sql: 'status=$2', value: 'superseded' },
    { name: 'archival', sql: 'archived_at=$2::timestamptz', value: instant },
    { name: 'eligible content revision', sql: 'data=$2', value: 'New still-eligible content' }
  ];
  async function mutate(id: string, change: typeof mutations[number]) {
    await pool.query(`UPDATE memories SET ${change.sql} WHERE id=$1`, [id, change.value]);
  }
  it.each(mutations.flatMap(change => ['json', 'bundle_v3'].map(format => ({ ...change, format }))))(
    'rechecks direct snapshot after $name race, $format', async change => {
      const candidate = await memory({ type: 'workflow' });
      after.set('semantic', () => mutate(candidate.id, change));
      const response = await recall({}, change.format);
      expect(records(response, change.format)).toEqual([]);
      expect(observed('semantic')[0].ids).toEqual([candidate.id]);
      expect(observed('final')[0].ids).toEqual([]);
      expect(await counts()).toEqual([{ id: candidate.id, recall_count: 0, revision: '2', recalled: false }]);
    }
  );

  it.each(['json', 'bundle_v3'].flatMap(format => ['historical', 'future'].map(window => ({ format, window }))))(
    'drops a $window factual-to-decision type race in factual mode, $format', async ({ format, window }) => {
      const candidate = await memory({ type: 'system_fact', ...(window === 'historical' ? { until: yesterday } : { from: tomorrow }) });
      after.set('semantic', async () => { await pool.query("UPDATE memories SET type='decision' WHERE id=$1", [candidate.id]); });
      const response = await recall({ mode: 'factual' }, format);
      expect(records(response, format)).toEqual([]);
      expect(observed('semantic')[0].ids).toEqual([candidate.id]);
      expect(observed('final')[0].ids).toEqual([]);
      expect((await counts())[0]).toMatchObject({ revision: '2', recall_count: 0, recalled: false });
    }
  );

  it.each(['semantic', 'graph'] as const)('rechecks expired graph origins after the %s query, not only destination revisions', async stage => {
    const seed = await memory({ type: 'constraint' });
    const neighbor = await memory({ vector: orthogonal() }); await edge(seed.id, neighbor.id);
    after.set(stage, () => mutate(seed.id, mutations[0]));
    const response = await recall();
    expect(response.memories).toEqual([]); expect(response.related_memories).toEqual([]);
    expect(observed('semantic')[0].ids).toEqual([seed.id]);
    expect(observed('graph')[0].ids).toEqual(stage === 'semantic' ? [] : [neighbor.id]);
    expect(observed('final')[0].ids).toEqual([]);
    expect((await counts()).every(row => row.recall_count === 0)).toBe(true);
  });

  it.each(mutations)('rechecks graph destination $name at the final snapshot', async change => {
    const seed = await memory(), neighbor = await memory({ type: 'workflow', vector: orthogonal() });
    await edge(seed.id, neighbor.id);
    after.set('graph', () => mutate(neighbor.id, change));
    const response = await recall();
    expect(response.memories.map((row: any) => row.id)).toEqual([seed.id]);
    expect(response.related_memories).toEqual([]);
    expect(observed('graph')[0].ids).toEqual([neighbor.id]);
    expect(observed('final')[0].ids).toEqual([seed.id]);
    expect((await counts()).find(row => row.id === neighbor.id)).toMatchObject({ recall_count: 0, revision: '2' });
  });

  it.each(['origin', 'destination'] as const)('excludes historical factual-to-behavioral type changes at graph %s', async target => {
    const seed = await memory({ until: yesterday });
    const neighbor = await memory({ until: yesterday, vector: orthogonal() }); await edge(seed.id, neighbor.id);
    const id = target === 'origin' ? seed.id : neighbor.id;
    after.set('graph', async () => { await pool.query("UPDATE memories SET type='decision' WHERE id=$1", [id]); });
    const response = await recall();
    expect(response.memories.map((row: any) => row.id)).toEqual(target === 'origin' ? [] : [seed.id]);
    expect(response.related_memories).toEqual([]);
    expect(observed('graph')[0].ids).toEqual([neighbor.id]);
    expect((await counts()).find(row => row.id === id)).toMatchObject({ recall_count: 0, revision: '2' });
  });

  it('rejects a still-eligible revised graph origin, even though its neighbor is unchanged', async () => {
    const seed = await memory(), neighbor = await memory({ vector: orthogonal() }); await edge(seed.id, neighbor.id);
    after.set('graph', () => mutate(seed.id, mutations[6]));
    const response = await recall();
    expect(response.memories).toEqual([]); expect(response.related_memories).toEqual([]);
    expect((await counts()).find(row => row.id === neighbor.id)).toMatchObject({ revision: '1', recall_count: 0 });
  });

  it.each(['edge', 'entitlement'] as const)('drops graph results when the %s disappears before the final snapshot', async target => {
    const seed = await memory(), neighbor = await memory({ vector: orthogonal() }); await edge(seed.id, neighbor.id);
    after.set('graph', async () => {
      if (target === 'edge') await pool.query('DELETE FROM memory_edges WHERE vault_id=$1', [state.vault.id]);
      else await pool.query(`UPDATE vaults SET rate_limit_override='{"curator_enabled":false}' WHERE id=$1`, [state.vault.id]);
    });
    const response = await recall();
    expect(response.memories.map((row: any) => row.id)).toEqual([seed.id]); expect(response.related_memories).toEqual([]);
    expect(observed('graph')[0].ids).toEqual([neighbor.id]);
  });

  it('retains a graph result supported by another unchanged eligible seed', async () => {
    const stale = await memory({ type: 'decision' }), live = await memory({ type: 'decision' });
    const neighbor = await memory({ vector: orthogonal() }); await edge(stale.id, neighbor.id); await edge(live.id, neighbor.id);
    after.set('graph', () => mutate(stale.id, mutations[0]));
    const response = await recall();
    expect(response.memories.map((row: any) => row.id)).toEqual([live.id]);
    expect(response.related_memories.map((row: any) => row.id)).toEqual([neighbor.id]);
  });

  it('uses one captured request instant across midnight, including graph and bundle assembly', async () => {
    vi.setSystemTime(new Date('2026-09-19T23:59:59.999Z'));
    const seed = await memory({ type: 'workflow', until: today });
    const neighbor = await memory({ type: 'decision', until: today, vector: orthogonal() }); await edge(seed.id, neighbor.id);
    after.set('semantic', async () => { vi.setSystemTime(new Date('2026-09-20T00:00:00.001Z')); });
    const response = await recall({}, 'bundle_v3');
    expect(records(response, 'bundle_v3').map(contentOf)).toEqual([seed.data, neighbor.data]);
    for (const phase of ['semantic', 'graph', 'final'] as const) expect(observed(phase)[0].args[4]).toBe('2026-09-19T23:59:59.999Z');
  });

  it.each(['expired', 'future', 'revised', 'historical-fact'] as const)('marks conflicts only with eligible revision-matched companions: %s', async kind => {
    const winner = await memory({ type: 'user_rule' });
    const other = await memory({ type: kind === 'historical-fact' ? 'system_fact' : 'constraint', vector: orthogonal(),
      ...(kind === 'expired' || kind === 'historical-fact' ? { until: yesterday } : kind === 'future' ? { from: tomorrow } : {}) });
    await pool.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b)
      VALUES($1,$2,$3,'keep_both',0.9,1,1)`, [state.vault.id, winner.id, other.id]);
    if (kind === 'revised') await mutate(other.id, mutations[6]);
    const response = await recall({ top_k: 1 }, 'bundle_v3');
    const rows = records(response, 'bundle_v3'); expect(rows).toHaveLength(1);
    expect(Boolean(rows[0].uncertainty)).toBe(kind === 'historical-fact');
  });

  it('fetches evidence only for final returned memories, not expired or raced-out candidates', async () => {
    const liveSource = await source('Evidence for current behavior'), expiredSource = await source('Evidence for expired behavior');
    const racedSource = await source('Evidence for superseded snapshot');
    const live = await memory({ type: 'constraint', sources: [liveSource] });
    const expired = await memory({ type: 'decision', until: yesterday, sources: [expiredSource] });
    const raced = await memory({ type: 'workflow', sources: [racedSource] });
    after.set('semantic', () => mutate(raced.id, mutations[0]));
    const response = await recall({ include_evidence: true });
    expect(response.memories.map((row: any) => row.id)).toEqual([live.id]);
    expect(response.evidence_chunks.map((row: any) => row.id)).toEqual([liveSource]); expect(response.raw_chunks).toEqual([]);
    expect(state.readBlob.mock.calls).toEqual([[liveSource]]);
    expect((await counts()).filter(row => row.recall_count === 0).map(row => row.id).sort()).toEqual([expired.id, raced.id].sort());
  });

  it('does not read evidence or increment counters when every candidate is expired', async () => {
    const chunk = await source('Expired source'); await memory({ type: 'user_rule', until: yesterday, sources: [chunk] });
    const response = await recall({ include_evidence: true });
    expect(response.memories).toEqual([]); expect(response.evidence_chunks).toEqual([]);
    expect(state.readBlob).not.toHaveBeenCalled(); expect(observed('counter')).toEqual([]);
  });

  it.each([0, 100, 800])('applies bundle byte budget %i after exclusions and counts only rendered records', async budget => {
    const expired = await memory({ type: 'user_rule', until: yesterday, data: 'Expired instruction' });
    const tooLarge = await memory({ type: 'user_preference', data: '日本語'.repeat(300) });
    const small = await memory({ type: 'system_fact', data: 'Short useful fact' });
    const response = await recall({ top_k: 2, max_bundle_bytes: budget }, 'bundle_v3');
    expect(Buffer.byteLength(response.bundle)).toBeLessThanOrEqual(budget);
    expect(records(response, 'bundle_v3').map(contentOf)).toEqual(budget === 800 ? [small.data] : []);
    if (response.bundle) expect(response.bundle.endsWith('</persistio_context>')).toBe(true);
    for (const row of await counts()) {
      expect(row.recall_count).toBe(row.id === small.id && budget === 800 ? 1 : 0);
      expect(row.revision).toBe('1');
    }
    expect(observed('semantic')[0].ids.sort()).toEqual([tooLarge.id, small.id].sort());
    expect(observed('semantic')[0].ids).not.toContain(expired.id);
  });

  async function enableEncryption() {
    const key = Buffer.alloc(32, 37); // Synthetic test-only DEK, never a service credential.
    config.ENCRYPTION_ENABLED = true; config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    const wrapped = Buffer.from('synthetic-temporal-key').toString('base64');
    await pool.query('UPDATE vaults SET encrypted_dek=$2,vault_encryption_enabled=true WHERE id=$1', [state.vault.id, wrapped]);
    state.vault = (await pool.query('SELECT * FROM vaults WHERE id=$1', [state.vault.id])).rows[0];
    state.unwrap.mockResolvedValue([{ plaintext: key }]);
    await crypt.initCryptoClient();
    return key;
  }
  it.each(['json', 'bundle_v3'])('filters before authenticated decryption and decrypts eligible data/subject, %s', async format => {
    const key = await enableEncryption();
    const live = await memory({ type: 'workflow', until: today, data: crypt.encryptField('Current encrypted workflow', key),
      subject: crypt.computeSubjectHmac('Private subject', key), encryptedSubject: crypt.encryptField('Private subject', key) });
    // Invalid ciphertext is deliberate: attempting to decrypt excluded rows must
    // fail, not silently hide a broken test behind an empty/fake crypto transform.
    const expired = await memory({ type: 'decision', until: yesterday, data: 'not-authenticated-ciphertext' });
    const future = await memory({ type: 'constraint', from: tomorrow, data: 'not-authenticated-ciphertext' });
    const neighbor = await memory({ type: 'system_fact', until: yesterday, vector: orthogonal(),
      data: crypt.encryptField('Encrypted historical knowledge', key), subject: 'subject digest',
      encryptedSubject: crypt.encryptField('Historical subject', key) });
    await edge(live.id, neighbor.id); await edge(live.id, expired.id); await edge(live.id, future.id);
    const response = await recall({}, format), rows = records(response, format);
    expect(rows.map(contentOf)).toEqual(['Current encrypted workflow', 'Encrypted historical knowledge']);
    expect(rows.map(row => row.subject)).toEqual(['Private subject', 'Historical subject']);
    expect(rows.map(row => row.temporal)).toEqual(['current_or_unbounded', 'historical']);
    expect(state.unwrap).toHaveBeenCalledOnce();
    expect(JSON.stringify(response)).not.toContain(live.data);
    for (const row of rows) { expect(row).not.toHaveProperty('subject_encrypted'); expect(row).not.toHaveProperty('encrypted_dek'); }
  });

  it('decrypts supporting evidence only after temporal and final snapshot filtering', async () => {
    const key = await enableEncryption();
    const chunk = await source(crypt.encryptField('Current encrypted evidence', key));
    const excludedChunk = await source('invalid-ciphertext-that-must-not-be-read');
    await memory({ type: 'constraint', data: crypt.encryptField('Current encrypted rule', key), sources: [chunk] });
    await memory({ type: 'decision', until: yesterday, data: 'invalid-ciphertext', sources: [excludedChunk] });
    const response = await recall({ include_evidence: true });
    expect(response.evidence_chunks).toMatchObject([{ id: chunk, content: 'Current encrypted evidence' }]);
    expect(state.readBlob.mock.calls).toEqual([[chunk]]); expect(state.unwrap).toHaveBeenCalledOnce();
  });

  it.each(['flag', 'wrapped-key'] as const)('rejects changed encryption %s at final snapshot before decrypting or counting', async change => {
    await enableEncryption();
    await memory({ type: 'constraint', data: 'bad-ciphertext-if-decrypted' });
    after.set('semantic', async () => {
      await pool.query(change === 'flag' ? 'UPDATE vaults SET vault_encryption_enabled=false WHERE id=$1'
        : "UPDATE vaults SET encrypted_dek='synthetic-rotated-key' WHERE id=$1", [state.vault.id]);
    });
    const response = await recall(); expect(response.memories).toEqual([]);
    expect(observed('final')[0].ids).toEqual([]); expect(observed('counter')).toEqual([]);
  });
});
