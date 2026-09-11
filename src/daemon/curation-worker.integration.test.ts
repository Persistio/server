import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({ vector: [] as number[], embed: vi.fn(), unwrap: vi.fn() }));
vi.mock('node:worker_threads', async original => ({ ...(await original() as typeof import('node:worker_threads')), parentPort: null }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed: state.embed }) }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = state.unwrap; } }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('actual curation worker lifecycle (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  const queues: string[] = [];
  let processBatch: typeof import('./curation-worker').processBatch;
  let service: typeof import('../services/curator').CuratorService;
  let config: ReturnType<typeof import('../config').getConfig>;
  let closePool: () => Promise<void>;
  let completion: ReturnType<typeof vi.spyOn>;

  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    process.env.CURATOR_API_KEY = 'local-test-model';
    const db = await import('../db/client');
    closePool = db.closePool;
    await db.runMigrations();
    config = (await import('../config')).getConfig();
    config.ENCRYPTION_ENABLED = false;
    config.CURATION_BATCH_SIZE = 1;
    config.CONTRADICTION_SCAN_ENABLED = true;
    config.GLOBAL_RULE_POLICY = 'approved_only';
    const type = (await pool.query("SELECT format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid='memories'::regclass AND attname='embedding'")).rows[0].type;
    const dimensions = Number(/\((\d+)\)/.exec(type)![1]);
    state.vector = [1, ...Array(dimensions - 1).fill(0)];
    state.embed.mockImplementation(async () => state.vector);
    service = (await import('../services/curator')).CuratorService;
    ({ processBatch } = await import('./curation-worker'));
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    config.CURATION_BATCH_SIZE = 1;
    config.ENCRYPTION_ENABLED = false;
    state.embed.mockReset().mockImplementation(async () => state.vector);
    await pool.query('DELETE FROM worker_action_receipts WHERE queue_id = ANY($1::uuid[])', [queues]);
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [vaults]);
    vaults.length = 0;
    queues.length = 0;
  });
  afterAll(async () => { await pool.end(); await closePool?.(); });

  async function fixture(count: number, cap = 12000, activeCount = 0, long = false) {
    const vault = crypto.randomUUID(), segment = crypto.randomUUID(), queue = crypto.randomUUID();
    vaults.push(vault); queues.push(queue);
    await pool.query(`INSERT INTO vaults (id,name,api_key_hash,plan_id,rate_limit_override)
      VALUES ($1,'curation-worker-regression',$2,'unlimited',$3::jsonb)`, [vault, crypto.randomUUID(),
      JSON.stringify({ curator_input_tokens_per_call: cap, curator_candidates_per_call: 40, curator_active_memories_per_call: 80 })]);
    await pool.query(`INSERT INTO segments (id,vault_id,session_id,project_id,chunk_ids,context)
      VALUES ($1,$2,'curation-worker','project','{}','User supplied facts for review.')`, [segment, vault]);
    const candidates: string[] = [], active: string[] = [];
    for (const [status, ids, n] of [['candidate', candidates, count], ['active', active, activeCount]] as const) {
      for (let i = 0; i < n; i++) {
        const id = crypto.randomUUID(); ids.push(id);
        await pool.query(`INSERT INTO memories (id,vault_id,data,subject,hash,scope,scope_key,status,source_segment_id,
          type,confidence,salience,embedding) VALUES ($1,$2,$3,$4,$5,'project','project',$6,$7,'system_fact',0.9,0.8,$8::vector)`,
        [id, vault, long ? `Fact ${i}. `.repeat(150) : `Fact ${status} ${i}.`, `topic-${i}`, crypto.randomUUID(), status,
          status === 'candidate' ? segment : null, long ? null : JSON.stringify(state.vector)]);
      }
    }
    await pool.query('INSERT INTO curation_queue (id,vault_id,segment_id) VALUES ($1,$2,$3)', [queue, vault, segment]);
    return { vault, segment, queue, candidates, active };
  }
  const empty = () => ({ schema_version: 'curation-plan.v1', nodes_to_create: [], nodes_to_update: [],
    nodes_to_archive: [], edges_to_create: [], promoted_candidates: [], discarded_candidates: [] } as any);
  function model(plan: (c: string[], m: string[]) => unknown | Promise<unknown>) {
    completion = vi.spyOn(service.prototype as any, 'createChatCompletion').mockImplementation(async (request: any) => {
      const parts = request.messages[1].content;
      const aliases = (index: number, prefix: string) => [...parts[index].text.matchAll(new RegExp(`^ID: (${prefix}\\d+)$`, 'gm'))].map((m: any) => m[1]);
      const result = await plan(aliases(0, 'C'), aliases(1, 'M'));
      return { usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 },
        choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(result) } }] };
    });
  }
  const create = (subject: string, source: string, parent?: string) => ({ subject, statement: `Created ${subject}`,
    type: 'system_fact', scope: 'project', evidence: 'source supports', consumed_candidate_ids: [source], parent_subject: parent });
  async function schedule(id: string) {
    return (await pool.query("SELECT authority_ready FROM memory_contradiction_schedule WHERE memory_id=$1 AND policy='approved_only'", [id])).rows;
  }

  it('reviews only visible candidate/context pairs, retains omitted work and accounts exactly once', async () => {
    const f = await fixture(20, 2000, 20, true);
    let reviewed = 0;
    model((c, m) => {
      reviewed = c.length;
      expect(m.length).toBe(c.length);
      return { ...empty(), promoted_candidates: c.map(id => ({ id, evidence: 'reviewed visible record' })) };
    });
    await processBatch();
    expect(completion).toHaveBeenCalledOnce();
    expect(reviewed).toBeGreaterThan(0); expect(reviewed).toBeLessThan(20);
    const counts = (await pool.query(`SELECT status,count(*)::int AS count FROM memories WHERE source_segment_id=$1 GROUP BY status`, [f.segment])).rows;
    expect(counts).toEqual(expect.arrayContaining([{ status: 'candidate', count: 20 - reviewed }, { status: 'active', count: reviewed }]));
    expect((await pool.query('SELECT retry_count,claim_token FROM curation_queue WHERE id=$1', [f.queue])).rows).toEqual([{ retry_count: 0, claim_token: null }]);
    expect((await pool.query('SELECT curator_candidates_processed,curator_requests FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0])
      .toEqual({ curator_candidates_processed: reviewed, curator_requests: 1 });
    const audit = (await pool.query('SELECT * FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(audit.validation_status).toBe('applied');
    expect(audit.raw_response.request.candidate_ids).toHaveLength(reviewed);
    expect(audit.before_state).toHaveLength(reviewed * 2);
    expect(audit.raw_response.request.deferred_candidate_ids).toHaveLength(20 - reviewed);
    expect((await pool.query('SELECT accounted_at IS NOT NULL AS accounted FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rows).toEqual([{ accounted: true }]);
    await processBatch(); // Deferred queue must not be immediately reclaimed or charged again.
    expect(completion).toHaveBeenCalledOnce();
  });

  it('defers impossible capacity without model spending, retries, receipts or deletion', async () => {
    const f = await fixture(1, 100);
    model(() => { throw new Error('Must not call the model'); });
    await processBatch();
    expect(completion).not.toHaveBeenCalled();
    expect((await pool.query('SELECT retry_count,last_error,available_at > now() AS deferred FROM curation_queue WHERE id=$1', [f.queue])).rows[0])
      .toMatchObject({ retry_count: 0, deferred: true, last_error: expect.stringContaining('mandatory contract') });
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
  });
  it('finishes empty work and dead-letters exhausted work through production fences', async () => {
    const emptyJob = await fixture(0), exhausted = await fixture(1);
    await pool.query('UPDATE curation_queue SET retry_count=100 WHERE id=$1', [exhausted.queue]);
    config.CURATION_BATCH_SIZE = 2;
    model(() => { throw new Error('No model call expected'); });
    await processBatch();
    expect(completion).not.toHaveBeenCalled();
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=ANY($1::uuid[])', [[emptyJob.queue, exhausted.queue]])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM curation_dead_letter WHERE vault_id=$1', [exhausted.vault])).rowCount).toBe(1);
    expect((await pool.query('SELECT action_key FROM worker_action_receipts WHERE queue_id=ANY($1::uuid[]) ORDER BY action_key', [[emptyJob.queue, exhausted.queue]])).rows)
      .toEqual([{ action_key: 'dead-letter' }, { action_key: 'empty-complete' }]);
  });

  it.each(['create', 'update', 'promote'] as const)('commits %s through real authority-held scheduling and approval/draining', async mode => {
    const f = await fixture(mode === 'create' ? 2 : 1, 12000, 1);
    model((c, m) => {
      const plan = empty();
      if (mode === 'create') {
        plan.promoted_candidates = [{ id: c[0], evidence: 'reviewed' }];
        plan.nodes_to_create = [create('child', c[1], c[0])];
        plan.edges_to_create = [{ from_subject: 'child', to_subject: c[0], type: 'supports', reason: 'relation' }];
      } else if (mode === 'update') plan.nodes_to_update = [{ id: m[0], subject: 'renamed', statement: 'Updated fact', reason: 'source', consumed_candidate_ids: c }];
      else plan.promoted_candidates = c.map(id => ({ id, evidence: 'reviewed' }));
      return plan;
    });
    await processBatch();
    const audit = (await pool.query('SELECT validation_status,validation_errors FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(audit, JSON.stringify(audit)).toEqual({ validation_status: 'applied', validation_errors: [] });
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=$1', [f.queue])).rowCount).toBe(0);
    const target = mode === 'create'
      ? (await pool.query("SELECT id,parent_id FROM memories WHERE vault_id=$1 AND subject='child'", [f.vault])).rows[0]
      : { id: mode === 'update' ? f.active[0] : f.candidates[0] };
    if (mode === 'create') {
      expect(f.candidates).toContain(target.parent_id);
      expect((await pool.query('SELECT from_memory_id,to_memory_id FROM memory_edges WHERE vault_id=$1', [f.vault])).rows)
        .toEqual([{ from_memory_id: target.id, to_memory_id: target.parent_id }]);
    }
    expect(await schedule(target.id)).toEqual([{ authority_ready: false }]);
    const drain = (await import('../services/contradiction-activation')).drainDueContradictionActivations;
    const extractor = { arbitrateConflict: vi.fn(async () => 'needs_review') };
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect(await schedule(target.id)).toHaveLength(1);
    // A real eligible neighbour makes draining prove scanner arbitration, not
    // merely removal of a schedule with no comparable memories.
    const peer = crypto.randomUUID();
    await pool.query(`INSERT INTO memories (id,vault_id,data,subject,hash,scope,scope_key,status,type,embedding)
      VALUES ($1,$2,'Previously approved comparable fact','peer',$3,'project','project','active','system_fact',$4::vector)`,
    [peer, f.vault, crypto.randomUUID(), JSON.stringify(state.vector)]);
    await pool.query("UPDATE memories SET authority_state='approved' WHERE id=$1", [peer]);
    await pool.query(`INSERT INTO memory_authority_events (vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason)
      SELECT vault_id,id,'approve','approved',authority_version,'user','api','test peer approval' FROM memories WHERE id=$1`, [peer]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE memory_id=$1', [peer]);
    await pool.query("UPDATE memories SET authority_state='approved' WHERE id=$1", [target.id]);
    await pool.query(`INSERT INTO memory_authority_events (vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason)
      SELECT vault_id,id,'approve','approved',authority_version,'user','api','test approval' FROM memories WHERE id=$1`, [target.id]);
    expect(await schedule(target.id)).toEqual([{ authority_ready: true }]);
    await drain(extractor as never);
    expect(extractor.arbitrateConflict).toHaveBeenCalledOnce();
    expect(await schedule(target.id)).toEqual([]);
  });

  it('does not start promotion, graph, receipt or schedules when embedding preparation fails', async () => {
    const f = await fixture(2);
    model(c => ({ ...empty(), promoted_candidates: [{ id: c[0], evidence: 'reviewed' }], nodes_to_create: [create('child', c[1], c[0])] }));
    state.embed.mockRejectedValue(new Error('injected embedding failure'));
    await processBatch();
    expect((await pool.query('SELECT status FROM memories WHERE source_segment_id=$1', [f.segment])).rows).toEqual([{ status: 'candidate' }, { status: 'candidate' }]);
    expect((await pool.query('SELECT 1 FROM memory_contradiction_schedule WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM memory_edges WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0].validation_status).toBe('application_failed');
    expect((await pool.query('SELECT curator_candidates_processed FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0]?.curator_candidates_processed ?? 0).toBe(0);
  });

  it.each(['forward-created-parent', 'renamed-active-parent'] as const)('applies compiled %s references without reinterpreting subjects', async mode => {
    const f = await fixture(2, 12000, mode === 'renamed-active-parent' ? 1 : 0);
    model(c => {
      const plan = empty();
      plan.nodes_to_create = [create('child', c[0], 'parent')];
      if (mode === 'forward-created-parent') plan.nodes_to_create.push(create('parent', c[1]));
      else plan.nodes_to_update = [{ id: 'M1', subject: 'parent', statement: 'Renamed parent fact', reason: 'support', consumed_candidate_ids: [c[1]] }];
      plan.edges_to_create = [{ from_subject: 'child', to_subject: 'parent', type: 'part_of', reason: 'hierarchy' }];
      return plan;
    });
    await processBatch();
    const audit = (await pool.query('SELECT validation_status,validation_errors FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(audit, JSON.stringify(audit)).toEqual({ validation_status: 'applied', validation_errors: [] });
    const rows = (await pool.query("SELECT id,subject,parent_id FROM memories WHERE vault_id=$1 AND status='active' ORDER BY subject", [f.vault])).rows;
    expect(rows.map(row => row.subject)).toEqual(['child', 'parent']);
    expect(rows[0].parent_id).toBe(rows[1].id);
    if (mode === 'renamed-active-parent') expect(rows[1].id).toBe(f.active[0]);
    expect((await pool.query('SELECT from_memory_id,to_memory_id FROM memory_edges WHERE vault_id=$1', [f.vault])).rows)
      .toEqual([{ from_memory_id: rows[0].id, to_memory_id: rows[1].id }]);
  });

  it.each(['revision', 'lease'] as const)('rejects a stale %s without applying the reviewed response', async kind => {
    const f = await fixture(1);
    model(async c => {
      if (kind === 'revision') await pool.query("UPDATE memories SET data='changed after review' WHERE id=$1", [f.candidates[0]]);
      else await pool.query("UPDATE curation_queue SET lease_expires_at=now()-interval '1 second' WHERE id=$1", [f.queue]);
      return { ...empty(), promoted_candidates: c.map(id => ({ id, evidence: 'reviewed' })) };
    });
    await processBatch();
    expect((await pool.query('SELECT status FROM memories WHERE id=$1', [f.candidates[0]])).rows[0].status).toBe('candidate');
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0].validation_status).toBe('application_failed');
  });

  it.each(['create', 'update'] as const)('holds no mutation locks while %s embedding is pending', async kind => {
    const f = await fixture(1, 12000, 1);
    model((c, m) => ({ ...empty(), ...(kind === 'create' ? { nodes_to_create: [create('new', c[0])] }
      : { nodes_to_update: [{ id: m[0], statement: 'Updated fact', reason: 'support', consumed_candidate_ids: c }] }) }));
    let started!: () => void, release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { started = resolve; });
    state.embed.mockImplementation(async () => { started(); await pending; return state.vector; });
    const work = processBatch(); await ready;
    const probe = await pool.connect();
    try {
      await probe.query('BEGIN');
      await probe.query('SELECT * FROM curation_queue WHERE id=$1 FOR UPDATE NOWAIT', [f.queue]);
      await probe.query('SELECT * FROM vault_curation_state WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
      await probe.query('SELECT * FROM vaults WHERE id=$1 FOR UPDATE NOWAIT', [f.vault]);
      await probe.query('SELECT * FROM memories WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
      expect((await probe.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
    } finally { await probe.query('ROLLBACK'); probe.release(); release(); }
    await work;
    expect(state.embed).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0].validation_status).toBe('applied');
  });
  it.each(['candidate', 'context', 'vault-lease'] as const)('rejects %s changes during embedding preparation for the whole action graph', async changed => {
    const f = await fixture(2, 12000, 1);
    model(c => ({ ...empty(), promoted_candidates: [{ id: c[0], evidence: 'reviewed' }], nodes_to_create: [create('child', c[1])] }));
    state.embed.mockImplementation(async () => {
      if (changed === 'vault-lease') await pool.query("UPDATE vault_curation_state SET curator_claimed_until=now()-interval '1 second' WHERE vault_id=$1", [f.vault]);
      else await pool.query("UPDATE memories SET data='Changed during embedding' WHERE id=$1", [changed === 'candidate' ? f.candidates[0] : f.active[0]]);
      return state.vector;
    });
    await processBatch();
    expect((await pool.query("SELECT 1 FROM memories WHERE source_segment_id=$1 AND status<>'candidate'", [f.segment])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_requests FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0]?.curator_requests ?? 0).toBe(0);
  });
  it('rolls back actions, audit and accounting when final queue deletion fails', async () => {
    const f = await fixture(1);
    model(c => ({ ...empty(), promoted_candidates: c.map(id => ({ id, evidence: 'reviewed' })) }));
    await pool.query(`CREATE FUNCTION pr370_fail_finish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF OLD.id='${f.queue}'::uuid THEN RAISE EXCEPTION 'injected finalization failure'; END IF; RETURN OLD; END $$`);
    await pool.query('CREATE TRIGGER pr370_fail_finish BEFORE DELETE ON curation_queue FOR EACH ROW EXECUTE FUNCTION pr370_fail_finish()');
    try { await processBatch(); }
    finally { await pool.query('DROP TRIGGER pr370_fail_finish ON curation_queue'); await pool.query('DROP FUNCTION pr370_fail_finish()'); }
    expect((await pool.query('SELECT status FROM memories WHERE id=$1', [f.candidates[0]])).rows[0].status).toBe('candidate');
    expect((await pool.query('SELECT 1 FROM worker_action_receipts WHERE queue_id=$1', [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_requests FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0]?.curator_requests ?? 0).toBe(0);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0].validation_status).toBe('application_failed');
  });
  it('does not repeat an old receipted batch, and defers its remaining work without another charge', async () => {
    const f = await fixture(1);
    const key = `apply-actions:${crypto.createHash('sha256').update(JSON.stringify({ reviewedCandidates: f.candidates.slice().sort() })).digest('hex')}`;
    await pool.query("INSERT INTO worker_action_receipts(queue_kind,queue_id,action_key,claim_token,accounted_at) VALUES ('curation',$1,$2,$3,now())", [f.queue, key, crypto.randomUUID()]);
    model(c => ({ ...empty(), promoted_candidates: c.map(id => ({ id, evidence: 'reviewed' })) }));
    const count = vi.spyOn(await import('../services/usage'), 'recordMemoryCountDelta');
    await processBatch();
    expect((await pool.query('SELECT status FROM memories WHERE id=$1', [f.candidates[0]])).rows[0].status).toBe('candidate');
    expect((await pool.query('SELECT claim_token,available_at>now() AS deferred FROM curation_queue WHERE id=$1', [f.queue])).rows[0]).toEqual({ claim_token: null, deferred: true });
    expect((await pool.query('SELECT curator_requests FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0]?.curator_requests ?? 0).toBe(0);
    expect(count).not.toHaveBeenCalled();
  });
  it('settles an old receipt using fresh empty state, not the model snapshot', async () => {
    const f = await fixture(1);
    const key = `apply-actions:${crypto.createHash('sha256').update(JSON.stringify({ reviewedCandidates: f.candidates })).digest('hex')}`;
    await pool.query("INSERT INTO worker_action_receipts(queue_kind,queue_id,action_key,claim_token,accounted_at) VALUES ('curation',$1,$2,$3,now())", [f.queue, key, crypto.randomUUID()]);
    model(async c => {
      await pool.query("UPDATE memories SET status='active' WHERE id=$1", [f.candidates[0]]);
      return { ...empty(), promoted_candidates: c.map(id => ({ id, evidence: 'reviewed' })) };
    });
    await processBatch();
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=$1', [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_requests FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0]?.curator_requests ?? 0).toBe(0);
  });
  it('keeps committed curation completed if count publication fails', async () => {
    const f = await fixture(1); model(c => ({ ...empty(), nodes_to_create: [create('new', c[0])] }));
    vi.spyOn(await import('../services/usage'), 'recordMemoryCountDelta').mockImplementation(() => { throw new Error('Count publisher failed'); });
    await processBatch();
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=$1', [f.queue])).rowCount).toBe(0);
    expect((await pool.query('SELECT validation_status FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0].validation_status).toBe('applied');
  });
  it.each(['budget', 'circuit'] as const)('atomically defers %s without retrying and refuses a stale deferral', async reason => {
    const { AiBudgetDeferredError } = await import('../services/usage');
    const { CircuitBreakerOpenError } = await import('../services/ai-resilience');
    const f = await fixture(1);
    model(() => { throw reason === 'budget' ? new AiBudgetDeferredError('curation', new Date(Date.now()+60_000), 60_000) : new CircuitBreakerOpenError('curator', 60_000); });
    await processBatch();
    expect((await pool.query('SELECT retry_count,claim_token,last_error FROM curation_queue WHERE id=$1', [f.queue])).rows[0])
      .toMatchObject({ retry_count: 0, claim_token: null, last_error: expect.any(String) });
    expect((await pool.query('SELECT last_curator_defer_reason FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].last_curator_defer_reason).toBeTruthy();
    await pool.query('UPDATE curation_queue SET available_at=now() WHERE id=$1', [f.queue]);
    await pool.query('UPDATE vault_curation_state SET next_curator_run_at=NULL,last_curator_defer_reason=NULL WHERE vault_id=$1', [f.vault]);
    model(async () => {
      await pool.query("UPDATE curation_queue SET lease_expires_at=now()-interval '1 second' WHERE id=$1", [f.queue]);
      throw new AiBudgetDeferredError('curation', new Date(Date.now()+60_000), 60_000);
    });
    await processBatch();
    expect((await pool.query('SELECT last_curator_defer_reason FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].last_curator_defer_reason).toBeNull();
    expect((await pool.query('SELECT retry_count FROM curation_queue WHERE id=$1', [f.queue])).rows[0].retry_count).toBe(0);
  });
  it('rolls back deferral usage/schedule/release together and stops every heartbeat on batch abort', async () => {
    const f = await fixture(1); await fixture(1); config.CURATION_BATCH_SIZE = 2;
    const { AiBudgetDeferredError } = await import('../services/usage');
    model(() => { throw new AiBudgetDeferredError('curation', new Date(Date.now()+60_000), 60_000); });
    // Applies to either claimed row: first scheduling write fails after its usage insert.
    await pool.query(`CREATE FUNCTION pr370_fail_deferral() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.next_curator_run_at IS NOT NULL THEN RAISE EXCEPTION 'deferral schedule failed'; END IF; RETURN NEW; END $$`);
    await pool.query('CREATE TRIGGER pr370_fail_deferral BEFORE UPDATE ON vault_curation_state FOR EACH ROW EXECUTE FUNCTION pr370_fail_deferral()');
    const starts = vi.spyOn(globalThis, 'setInterval'), stops = vi.spyOn(globalThis, 'clearInterval');
    try { await expect(processBatch()).rejects.toThrow('deferral schedule failed'); }
    finally { await pool.query('DROP TRIGGER pr370_fail_deferral ON vault_curation_state'); await pool.query('DROP FUNCTION pr370_fail_deferral()'); }
    const handles = starts.mock.results.filter(result => result.type === 'return').map(result => result.value);
    expect(handles).toHaveLength(2);
    for (const handle of handles) expect(stops).toHaveBeenCalledWith(handle);
    expect((await pool.query('SELECT 1 FROM vault_usage WHERE vault_id=ANY($1::uuid[])', [vaults])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE id=$1 AND claim_token IS NOT NULL AND retry_count=0', [f.queue])).rowCount).toBe(1);
    expect((await pool.query('SELECT next_curator_run_at FROM vault_curation_state WHERE vault_id=$1', [f.vault])).rows[0].next_curator_run_at).toBeNull();
  });
  it('processes several rows under the same vault claim without clearing it per row', async () => {
    const f = await fixture(1); config.CURATION_BATCH_SIZE = 2;
    const segment = crypto.randomUUID(), queue = crypto.randomUUID(); queues.push(queue);
    await pool.query("INSERT INTO segments(id,vault_id,session_id,project_id,chunk_ids) VALUES ($1,$2,'second','project','{}')", [segment, f.vault]);
    await pool.query("INSERT INTO memories(vault_id,data,subject,hash,scope,scope_key,status,source_segment_id,type) VALUES ($1,'Second segment fact','second',$2,'project','project','candidate',$3,'system_fact')", [f.vault, crypto.randomUUID(), segment]);
    await pool.query('INSERT INTO curation_queue(id,vault_id,segment_id) VALUES ($1,$2,$3)', [queue, f.vault, segment]);
    model(c => ({ ...empty(), promoted_candidates: c.map(id => ({ id, evidence: 'reviewed' })) }));
    await processBatch();
    expect(completion).toHaveBeenCalledTimes(2);
    expect((await pool.query('SELECT 1 FROM curation_queue WHERE vault_id=$1', [f.vault])).rowCount).toBe(0);
    expect((await pool.query('SELECT curator_runs,curator_requests FROM vault_usage WHERE vault_id=$1', [f.vault])).rows[0])
      .toEqual({ curator_runs: 1, curator_requests: 2 });
  });
  it('claims multiple vaults concurrently without duplicating ownership or deadlocking', async () => {
    await fixture(1); await fixture(1); await fixture(1); await fixture(1);
    const { claimEligibleCurationJobs } = await import('../services/curation-capacity');
    const results = await Promise.all([claimEligibleCurationJobs(2, 'claim-a'), claimEligibleCurationJobs(2, 'claim-b')]);
    const ids = results.flat().map(row => row.queue_id);
    expect(ids.length).toBeGreaterThan(0);
    expect(new Set(ids).size).toBe(ids.length);
  });
  it('applies encrypted curation with no KMS call under locks and no plaintext audit payload', async () => {
    const f = await fixture(1);
    const key = Buffer.alloc(32, 11);
    const crypt = await import('../services/crypto');
    config.KEY_PROVIDER = 'gcp_kms'; config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    config.ENCRYPTION_ENABLED = true;
    state.unwrap.mockReset().mockImplementation(async () => {
      const probe = await pool.connect();
      try {
        await probe.query('BEGIN');
        await probe.query('SELECT * FROM curation_queue WHERE id=$1 FOR UPDATE NOWAIT', [f.queue]);
        await probe.query('SELECT * FROM vault_curation_state WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
        await probe.query('SELECT * FROM memories WHERE vault_id=$1 FOR UPDATE NOWAIT', [f.vault]);
      } finally { await probe.query('ROLLBACK'); probe.release(); }
      return [{ plaintext: key }];
    });
    await crypt.initCryptoClient();
    await pool.query('UPDATE vaults SET encrypted_dek=$2,vault_encryption_enabled=true WHERE id=$1', [f.vault, Buffer.from('wrapped-fixture').toString('base64')]);
    await pool.query('UPDATE segments SET context=$2 WHERE id=$1', [f.segment, crypt.encryptField('User supplied facts for review.', key)]);
    await pool.query("UPDATE memories SET data=$2,subject='',subject_encrypted=$3,subject_hmac=$4 WHERE id=$1", [f.candidates[0],
      crypt.encryptField('Fact candidate 0.', key), crypt.encryptField('topic-0', key), crypt.computeSubjectHmac('topic-0', key)]);
    model(c => ({ ...empty(), nodes_to_create: [create('private topic', c[0])] }));
    await processBatch();
    const audit = (await pool.query('SELECT validation_status,raw_response,before_state,after_state FROM curation_review_runs WHERE vault_id=$1', [f.vault])).rows[0];
    expect(audit.validation_status).toBe('applied');
    expect(audit.raw_response).toHaveProperty('encrypted'); expect(audit.before_state).toHaveProperty('encrypted'); expect(audit.after_state).toHaveProperty('encrypted');
    expect(JSON.stringify(audit)).not.toContain('private topic');
    const memory = (await pool.query("SELECT data,subject_encrypted FROM memories WHERE vault_id=$1 AND status='active'", [f.vault])).rows[0];
    expect(crypt.decryptField(memory.data, key)).toBe('Created private topic');
    expect(crypt.decryptField(memory.subject_encrypted, key)).toBe('private topic');
    const logs = (await pool.query('SELECT subject,new_value,raw_curator_response FROM curation_action_log WHERE vault_id=$1', [f.vault])).rows;
    expect(JSON.stringify(logs)).not.toContain('private topic');
    expect(state.unwrap).toHaveBeenCalledTimes(2); // Read preparation + exact wrapped-key apply preparation; none under locks.
  });
});
