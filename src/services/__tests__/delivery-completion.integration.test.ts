import crypto from 'node:crypto';
import { Pool, type PoolClient } from 'pg';
import { beforeAll, afterAll, afterEach, describe, it, expect, vi } from 'vitest';

const state = vi.hoisted(() => ({ pool: null as Pool | null, fail: null as null | ((sql: string, params?: unknown[]) => boolean), ambiguous: false, delayHealth:false }));
vi.mock('../../db/client', () => ({
  withTransaction: async (work: (client: PoolClient) => Promise<unknown>) => {
    const client = await state.pool!.connect();
    const original = client.query.bind(client);
    const execute = async (sql: string, params?: unknown[]) => {
      if (state.fail?.(sql, params)) throw new Error('injected SQL failure');
      if (state.delayHealth && sql.startsWith('WITH cutoff')) return original('SELECT pg_sleep(3)');
      const result = await original(sql, params);
      if (sql === 'COMMIT' && state.ambiguous) throw new Error('ambiguous commit');
      return result;
    };
    try { await execute('BEGIN'); const value = await work({ query: execute } as PoolClient); await execute('COMMIT'); return value; }
    catch (error) { await original('ROLLBACK'); throw error; }
    finally { client.release(); }
  }
}));
import { recordRecallDelivery, recordRenderedDelivery, type RenderedDeliveryInput, type RecallDeliveryItem } from '../memory-observability';
import { readDeliveryHealth, createDeliveryHealthCollector } from '../delivery-health';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('delivery completion lifecycle (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  const runs: string[] = [];
  beforeAll(async () => {
    state.pool = pool;
    expect((await pool.query("SELECT 1 FROM schema_migrations WHERE filename='056_delivery_completion_and_audit_guards.sql'")).rowCount).toBe(1);
  });
  afterEach(() => { state.fail = null; state.ambiguous = false; state.delayHealth=false; });
  afterAll(async () => { await pool.end(); });
  async function delivery(count = 2) {
    const items: RecallDeliveryItem[] = Array.from({length:count}, () => ({ memoryId:crypto.randomUUID(),authorityVersion:1,
      section:'historical_facts',memoryType:'system_fact',retrievalReason:'semantic',scope:'global',scopeBinding:null,
      authorityState:'untrusted',authorityRequired:false,authorityApprovalValid:false,similarity:0.8 }));
    const id = await recordRecallDelivery({vaultId,query:'private query never stored',responseFormat:'json',mode:'agent',clientName:'test',clientVersion:'1',
      context:{},topK:Math.max(1,count),minSimilarity:0.5,globalRulePolicy:'off',includeGlobalRulesRequested:false,includeGlobalRulesEffective:false,items});
    runs.push(id);
    const input: RenderedDeliveryInput = {vaultId,deliveryId:id,renderedIds:items.map(item=>item.memoryId),dropped:[],tokenBudget:100,renderedTokens:10,truncated:false,renderTarget:'tool_response'};
    return { id,items,input };
  }
  async function counts(id: string) {
    return (await pool.query(`SELECT
      (SELECT count(*)::int FROM memory_delivery_events WHERE delivery_id=$1 AND stage IN ('rendered','dropped')) AS terminal,
      (SELECT count(*)::int FROM memory_delivery_acknowledgements WHERE delivery_id=$1) AS ack,
      (SELECT count(*)::int FROM memory_delivery_pending WHERE delivery_id=$1) AS pending`,[id])).rows[0];
  }
  it.each([0,1,2,100])('atomically completes %i selections, then accepts reordered/case-normalized exact retry', async count => {
    const {id,input}=await delivery(count);
    expect(await counts(id)).toEqual({terminal:0,ack:0,pending:1});
    expect((await recordRenderedDelivery(input)).inserted).toBe(count);
    expect(await counts(id)).toEqual({terminal:count,ack:1,pending:0});
    expect((await recordRenderedDelivery({...input,renderedIds:[...input.renderedIds].reverse().map(id=>id.toUpperCase())})).inserted).toBe(0);
    expect((await pool.query('SELECT outcome_hash,origin FROM memory_delivery_acknowledgements WHERE delivery_id=$1',[id])).rows[0]).toMatchObject({origin:'client_ack',outcome_hash:expect.stringMatching(/^[a-f0-9]{64}$/)});
  });
  it.each([0,2])('rejects every conflicting shared outcome even for %i memories', async count => {
    const {input}=await delivery(count);await recordRenderedDelivery(input);
    for(const changed of [{renderTarget:'prompt_context' as const},{tokenBudget:101},{renderedTokens:9},{truncated:true}]) {
      await expect(recordRenderedDelivery({...input,...changed})).rejects.toThrow(/different immutable/);
    }
  });
  it('records all-dropped output and rejects altered drop reasons', async () => {
    const {input,id}=await delivery();
    const dropped={...input,renderedIds:[],dropped:input.renderedIds.map(id=>({id,reason:'token_budget'})),truncated:true};
    expect((await recordRenderedDelivery(dropped)).dropped).toBe(2);
    await expect(recordRenderedDelivery({...dropped,dropped:dropped.dropped.map(item=>({...item,reason:'client_policy'}))})).rejects.toThrow(/different immutable/);
    expect(await counts(id)).toEqual({terminal:2,ack:1,pending:0});
  });
  it('records a partial render by identity without conflating drops or retrying them as renders',async()=>{
    const {input,id}=await delivery();const mixed={...input,renderedIds:[input.renderedIds[0]],dropped:[{id:input.renderedIds[1],reason:'token_budget'}],truncated:true};
    expect(await recordRenderedDelivery(mixed)).toMatchObject({inserted:2,rendered:1,dropped:1});
    expect((await recordRenderedDelivery({...mixed,renderedIds:[mixed.renderedIds[0].toUpperCase()]})).inserted).toBe(0);
    await expect(recordRenderedDelivery(input)).rejects.toThrow(/different immutable/);
    expect(await counts(id)).toEqual({terminal:2,ack:1,pending:0});
  });
  it('rejects cross-vault, missing, overlapping, duplicate and foreign partitions', async () => {
    const {input,id}=await delivery();
    await expect(recordRenderedDelivery({...input,vaultId:crypto.randomUUID()})).rejects.toThrow(/not found/);
    for(const changed of [{renderedIds:input.renderedIds.slice(0,1)}, {renderedIds:[input.renderedIds[0],input.renderedIds[0].toUpperCase()]},
      {dropped:[{id:input.renderedIds[0],reason:'invalid'}]}, {renderedIds:[crypto.randomUUID(),input.renderedIds[1]]},
      {renderedTokens:101}, {tokenBudget:2147483648}, {renderedTokens:0.5}]) await expect(recordRenderedDelivery({...input,...changed})).rejects.toThrow();
    expect(await counts(id)).toEqual({terminal:0,ack:0,pending:1});
  });
  it.each(['same','conflicting'])('serializes %s concurrent ACKs on separate connections', async kind => {
    const {input,id}=await delivery();
    const results=await Promise.allSettled([recordRenderedDelivery(input),recordRenderedDelivery(kind==='same'?input:{...input,truncated:true})]);
    expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(kind==='same'?2:1);
    expect(await counts(id)).toEqual({terminal:2,ack:1,pending:0});
  });
  it.each(['last_terminal','ack','commit'])('rolls back %s failure with no partial evidence', async stage => {
    const {input,id}=await delivery();let terminal=0;
    state.fail=(sql)=>stage==='last_terminal' ? sql.includes('INSERT INTO memory_delivery_events') && ++terminal===2
      : stage==='ack' ? sql.includes('INSERT INTO memory_delivery_acknowledgements') : sql==='COMMIT';
    await expect(recordRenderedDelivery(input)).rejects.toThrow(/injected/);state.fail=null;
    expect(await counts(id)).toEqual({terminal:0,ack:0,pending:1});
    expect((await recordRenderedDelivery(input)).inserted).toBe(2);
  });
  it('settles ambiguous COMMIT only by exact retry, not a second outcome', async () => {
    const {input,id}=await delivery();state.ambiguous=true;
    await expect(recordRenderedDelivery(input)).rejects.toThrow(/ambiguous/);state.ambiguous=false;
    expect(await counts(id)).toEqual({terminal:2,ack:1,pending:0});
    expect((await recordRenderedDelivery(input)).inserted).toBe(0);
  });
  it('rolls back terminal rows and run ACK when the derived pending removal fails inside SQL',async()=>{
    const {input,id}=await delivery();const name=`test_pending_failure_${crypto.randomUUID().replaceAll('-','')}`;
    await pool.query(`CREATE FUNCTION ${name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.delivery_id='${id}'::uuid THEN RAISE EXCEPTION 'injected pending removal'; END IF; RETURN OLD; END $$`);
    try{
      await pool.query(`CREATE TRIGGER ${name} BEFORE DELETE ON memory_delivery_pending FOR EACH ROW EXECUTE FUNCTION ${name}()`);
      await expect(recordRenderedDelivery(input)).rejects.toThrow(/injected pending removal/);
      expect(await counts(id)).toEqual({terminal:0,ack:0,pending:1});
    }finally{await pool.query(`DROP TRIGGER IF EXISTS ${name} ON memory_delivery_pending`);await pool.query(`DROP FUNCTION ${name}()`);}
    expect((await recordRenderedDelivery(input)).inserted).toBe(2);
  });
  it('rejects direct terminal snapshot forgery and cannot complete a partial or inconsistent set',async()=>{
    const {input,id}=await delivery();
    const insert=`INSERT INTO memory_delivery_events(delivery_id,vault_id,memory_id,authority_version,stage,section,memory_type,retrieval_reason,scope,scope_binding,authority_state,authority_required,authority_approval_valid,similarity,render_target,token_budget,rendered_tokens,truncated)
      SELECT delivery_id,vault_id,memory_id,authority_version + $2,'rendered',section,memory_type,retrieval_reason,scope,scope_binding,authority_state,authority_required,authority_approval_valid,similarity,'tool_response',100,10,false FROM memory_delivery_events WHERE delivery_id=$1 AND stage='selected' ORDER BY memory_id LIMIT 1`;
    await expect(pool.query(insert,[id,1])).rejects.toThrow(/differs from selected/);
    await pool.query(insert,[id,0]);await expect(recordRenderedDelivery(input)).rejects.toThrow(/inconsistent retained/);
    expect(await counts(id)).toEqual({terminal:1,ack:0,pending:1});
    expect((await pool.query('SELECT integrity_error FROM memory_delivery_pending WHERE delivery_id=$1',[id])).rows[0].integrity_error).toBe(true);
  });
  it('guards pending identity/classification/removal and post-ACK event insertions', async () => {
    const {input,id}=await delivery();
    await expect(pool.query('DELETE FROM memory_delivery_pending WHERE delivery_id=$1',[id])).rejects.toThrow(/requires an acknowledgement/);
    await expect(pool.query("UPDATE memory_delivery_pending SET created_at=created_at-interval '1 day' WHERE delivery_id=$1",[id])).rejects.toThrow(/identity/);
    await expect(pool.query('UPDATE memory_delivery_pending SET integrity_error=true WHERE delivery_id=$1',[id])).rejects.toThrow(/classification/);
    await recordRenderedDelivery(input);
    await expect(pool.query(`INSERT INTO memory_delivery_events SELECT gen_random_uuid(),delivery_id,vault_id,gen_random_uuid(),authority_version,
      stage,section,memory_type,retrieval_reason,scope,scope_binding,authority_state,authority_required,authority_approval_valid,similarity,
      drop_reason,render_target,token_budget,rendered_tokens,truncated,clock_timestamp() FROM memory_delivery_events WHERE delivery_id=$1 LIMIT 1`,[id])).rejects.toThrow(/cannot acquire/);
  });
  it('detects no callback on old pending runs, then clears with a late ACK', async () => {
    // Insert an aged empty run: no callbacks, no mutation of immutable timestamps.
    const id=crypto.randomUUID();runs.push(id);
    const before=await readDeliveryHealth();
    await pool.query(`INSERT INTO memory_delivery_runs(id,vault_id,query_hash,response_format,mode,top_k,min_similarity,global_rule_policy,
      include_global_rules_requested,include_global_rules_effective,selected_count,global_selected_count,created_at)
      VALUES($1,$2,$3,'json','agent',1,0.5,'off',false,false,0,0,clock_timestamp()-interval '6 minutes')`,[id,vaultId,'a'.repeat(64)]);
    expect((await readDeliveryHealth()).overdue).toBe(before.overdue+1);
    await recordRenderedDelivery({vaultId,deliveryId:id,renderedIds:[],dropped:[],tokenBudget:0,renderedTokens:0,truncated:false,renderTarget:'prompt_context'});
    expect((await readDeliveryHealth()).overdue).toBe(before.overdue);
  });
  it('cancels monitoring lock waits instead of emitting healthy zero', async () => {
    const blocker=await pool.connect();await blocker.query('BEGIN');await blocker.query('LOCK TABLE memory_delivery_pending IN ACCESS EXCLUSIVE MODE');
    const observed: unknown[]=[];const start=Date.now();
    try { await createDeliveryHealthCollector()({observe:(...value)=>observed.push(value)}); }
    finally { await blocker.query('ROLLBACK');blocker.release(); }
    expect(Date.now()-start).toBeLessThan(4000);expect(observed).toEqual([[1,{state:'monitor_error'}]]);
  });
  it('cancels a genuinely delayed PostgreSQL statement within the configured two-second bound',async()=>{
    state.delayHealth=true;const observed:unknown[]=[];const start=Date.now();
    await createDeliveryHealthCollector()({observe:(...value)=>observed.push(value)});
    expect(observed).toEqual([[1,{state:'monitor_error'}]]);expect(Date.now()-start).toBeLessThan(2800);
  });
});
